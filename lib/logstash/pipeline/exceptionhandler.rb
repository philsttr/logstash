class LogStash::Pipeline::ExceptionHandler
  java_implements com.lmax.disruptor.ExceptionHandler
  
  def initialize(name)
    @logger = Cabin::Channel.get(LogStash)
    @name = name
  end
  
  java_signature 'void handleEventException(Throwable ex, long sequence, Object event)'
  def handleEventException(exception, sequence, event)
    @logger.error("Exception in #{@name}", "exception" => exception, "backtrace" => exception.stackTrace, "event" => event.to_hash)
  end

  java_signature 'void handleOnStartException(Throwable ex)'
  def handleOnStartException(exception)
    @logger.error("Exception on startup in #{@name}", "exception" => exception, "backtrace" => exception.stackTrace)
  end

  java_signature 'void handleOnShutdownException(Throwable ex)'
  def handleOnShutdownException(exception)
    @logger.error("Exception on shutdown in #{@name}", "exception" => exception, "backtrace" => exception.stackTrace)
  end
end