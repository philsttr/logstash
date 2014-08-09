# encoding: utf-8
require "logstash/config/file"
require "logstash/namespace"
require "thread" # stdlib
require "logstash/filters/base"
require "logstash/inputs/base"
require "logstash/outputs/base"
require "logstash/errors"
require "logstash/eventpublishingqueue"
require "stud/interval" # gem stud
LogStash::Environment.load_disruptor_jars!
java_import com.lmax.disruptor.dsl.Disruptor
java_import java.util.concurrent.Executors
java_import com.lmax.disruptor.EventHandler

require "logstash/event.rb"
require "logstash/pipeline/filtereventhandler.rb"
require "logstash/pipeline/outputeventhandler.rb"
require "logstash/pipeline/exceptionhandler.rb"

class LogStash::Pipeline::Pipeline
  def initialize(configstr)
    @logger = Cabin::Channel.get(LogStash)
    grammar = LogStashConfigParser.new
    @config = grammar.parse(configstr)
    if @config.nil?
      raise LogStash::ConfigurationError, grammar.failure_reason
    end

    # This will compile the config to ruby and evaluate the resulting code.
    # The code will initialize all the plugins and define the
    # filter and output methods.
    code = @config.compile
    # The config code is hard to represent as a log message...
    # So just print it.
    @logger.debug? && @logger.debug("Compiled pipeline code:\n#{code}")
    begin
      eval(code)
    rescue => e
      raise
    end
    
    @settings = {
      "filter-workers" => 1,
    }
  end # def initialize

  def ready?
    return @ready
  end

  def started?
    return @started
  end

  def configure(setting, value)
    if setting == "filter-workers"
      # Abort if we have any filters that aren't threadsafe
      if value > 1 && @filters.any? { |f| !f.threadsafe? }
        plugins = @filters.select { |f| !f.threadsafe? }.collect { |f| f.class.config_name }
        raise LogStash::ConfigurationError, "Cannot use more than 1 filter worker because the following plugins don't work with more than one worker: #{plugins.join(", ")}"
      end
    end
    @settings[setting] = value
  end

  def filters?
    return @filters.any?
  end

  def run
    @started = true
    
    # TODO (philsttr) handle different wait strategies
    disruptor = Disruptor.new(LogStashEventFactory.new, 512, Executors.newCachedThreadPool)
    
    filterHandler = LogStash::Pipeline::FilterEventHandler.new(self)
    outputHandler = LogStash::Pipeline::OutputEventHandler.new(self)
    disruptor.handleEventsWith([filterHandler].to_java(EventHandler)).then([outputHandler].to_java(EventHandler))
    
    disruptor.handleExceptionsFor(filterHandler).with(LogStash::Pipeline::ExceptionHandler.new("filter"));
    disruptor.handleExceptionsFor(outputHandler).with(LogStash::Pipeline::ExceptionHandler.new("output"));
    
    register_filters if filters?
    register_outputs

    disruptor.start
    
    @event_publishing_queue = LogStash::EventPublishingQueue.new
    # Force new events to be retrieved from the ring_buffer instead of allocating new objects
    LogStash::Event.initialize_new_events_from(disruptor.ringBuffer)
    
    @input_threads = []
    start_inputs

    @ready = true

    @logger.info("Pipeline started")
    wait_inputs

    disruptor.shutdown
    
    # Allocate new events, instead of retrieving them from the ring_buffer (useful for unit tests)
    LogStash::Event.initialize_new_events_from_allocation
    
    teardown_filters if filters?
    teardown_outputs

    @logger.info("Pipeline shutdown complete.")

    # exit code
    return 0
  end # def run

  def wait_inputs
    @input_threads.each(&:join)
  rescue Interrupt
    # rbx does weird things during do SIGINT that I haven't debugged
    # so we catch Interrupt here and signal a shutdown. For some reason the
    # signal handler isn't invoked it seems? I dunno, haven't looked much into
    # it.
    shutdown
  end

  def start_inputs
    moreinputs = []
    @inputs.each do |input|
      if input.threadable && input.threads > 1
        (input.threads-1).times do |i|
          moreinputs << input.clone
        end
      end
    end
    @inputs += moreinputs

    @inputs.each do |input|
      input.register
      start_input(input)
    end
  end

  def start_input(plugin)
    @input_threads << Thread.new { inputworker(plugin) }
  end

  def inputworker(plugin)
    LogStash::Util::set_thread_name("<#{plugin.class.config_name}")
    begin
      if plugin.method(:run).arity == 0
        # Recommended functionality where inputs just create events and call event.publish
        plugin.run
      else
        # Backwards compatibility with inputs that pushed events onto a queue
        plugin.run(@event_publishing_queue)
      end
    rescue LogStash::ShutdownSignal
      return
    rescue => e
      if @logger.debug?
        @logger.error(I18n.t("logstash.pipeline.worker-error-debug",
                             :plugin => plugin.inspect, :error => e.to_s,
                             :exception => e.class,
                             :stacktrace => e.backtrace.join("\n")))
      else
        @logger.error(I18n.t("logstash.pipeline.worker-error",
                             :plugin => plugin.inspect, :error => e))
      end
      puts e.backtrace if @logger.debug?
      plugin.teardown
      sleep 1
      retry
    end
  rescue LogStash::ShutdownSignal
    # nothing
  ensure
    plugin.teardown
  end # def inputworker

  def register_filters
    @filters.each(&:register)
    # Set up the periodic flusher thread.
    @flusher_thread = Thread.new { Stud.interval(5) { filter_flusher } }
  end
  
  def teardown_filters
    @filters.each(&:teardown)
  end
  
  def register_outputs
    @outputs.each(&:register)
    @outputs.each(&:worker_setup)
  end
  
  def teardown_outputs
    @outputs.each(&:teardown)
  end
    
  
  # Shutdown this pipeline.
  #
  # This method is intended to be called from another thread
  def shutdown
    @input_threads.each do |thread|
      # Interrupt all inputs
      @logger.info("Sending shutdown signal to input thread",
                   :thread => thread)
      thread.raise(LogStash::ShutdownSignal)
      begin
        thread.wakeup # in case it's in blocked IO or sleeping
      rescue ThreadError
      end

      # Sometimes an input is stuck in a blocking I/O
      # so we need to tell it to teardown directly
      @inputs.each do |input|
        input.teardown
      end
    end
    
    # No need to send the ShutdownSignal to the filters/outputs nor to wait for
    # the inputs to finish, because in the #run method we wait for that anyway.
  end # def shutdown

  def plugin(plugin_type, name, *args)
    args << {} if args.empty?
    klass = LogStash::Plugin.lookup(plugin_type, name)
    return klass.new(*args)
  end

  def filter(event, end_of_batch = true, &block)
    @filter_func.call(event, end_of_batch, &block)
  end
  
  def output(event, end_of_batch = true)
    @output_func.call(event, end_of_batch)
  end

  def filter_flusher
    LogStash::Util::set_thread_name("filterflusher")
    @filters.each do |filter|
      # TODO(sissel): watchdog on flushes?
      if filter.respond_to?(:flush)
        filter.flush do |event|
          event.publish
        end
      end
    end
  end # def filter_flusher
  
  class LogStashEventFactory
    java_implements com.lmax.disruptor.EventFactory
    java_signature 'Object newInstance()'
    def newInstance
      raise "Unable to allocate a new LogStash::Event.  This typically means that either multiple Pipelines have been started, or a previous Pipeline instance was not shutdown properly." if LogStash::Event.initializing_from_ring_buffer?
      return LogStash::Event.new
    end
  end
  
end # class Pipeline

