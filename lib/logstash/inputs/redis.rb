# encoding: utf-8
require "logstash/inputs/base"
require "logstash/inputs/threadable"
require "logstash/namespace"

# This input will read events from a Redis instance; it supports both Redis channels and lists.
# The list command (BLPOP) used by Logstash is supported in Redis v1.3.1+, and
# the channel commands used by Logstash are found in Redis v1.3.8+. 
# While you may be able to make these Redis versions work, the best performance
# and stability will be found in more recent stable versions.  Versions 2.6.0+
# are recommended.
#
# For more information about Redis, see <http://redis.io/>
#
# `batch_count` note: If you use the `batch_count` setting, you *must* use a Redis version 2.6.0 or
# newer. Anything older does not support the operations used by batching.
#
class LogStash::Inputs::Redis < LogStash::Inputs::Threadable
  config_name "redis"
  milestone 2

  default :codec, "json"

  # The `name` configuration is used for logging in case there are multiple instances.
  # This feature has no real function and will be removed in future versions.
  config :name, :validate => :string, :default => "default", :deprecated => true

  # The hostname of your Redis server.
  config :host, :validate => :string, :default => "127.0.0.1"

  # The port to connect on.
  config :port, :validate => :number, :default => 6379

  # The Redis database number.
  config :db, :validate => :number, :default => 0

  # Initial connection timeout in seconds.
  config :timeout, :validate => :number, :default => 5

  # Password to authenticate with. There is no authentication by default.
  config :password, :validate => :password

  # The name of the Redis queue (we'll use BLPOP against this).
  # TODO: remove soon.
  config :queue, :validate => :string, :deprecated => true

  # The name of a Redis list or channel.
  # TODO: change required to true
  config :key, :validate => :string, :required => false

  # Specify either list or channel.  If `redis\_type` is `list`, then we will BLPOP the
  # key.  If `redis\_type` is `channel`, then we will SUBSCRIBE to the key.
  # If `redis\_type` is `pattern_channel`, then we will PSUBSCRIBE to the key.
  # TODO: change required to true
  config :data_type, :validate => [ "list", "channel", "pattern_channel" ], :required => false

  # The number of events to return from Redis using EVAL.
  config :batch_count, :validate => :number, :default => 1

  public
  def register
    require 'redis'
    @redis = nil
    @redis_url = "redis://#{@password}@#{@host}:#{@port}/#{@db}"

    # TODO remove after setting key and data_type to true
    if @queue
      if @key or @data_type
        raise RuntimeError.new(
          "Cannot specify queue parameter and key or data_type"
        )
      end
      @key = @queue
      @data_type = 'list'
    end

    if not @key or not @data_type
      raise RuntimeError.new(
        "Must define queue, or key and data_type parameters"
      )
    end
    # end TODO

    @logger.info("Registering Redis", :identity => identity)
  end # def register

  # A string used to identify a Redis instance in log messages
  # TODO(sissel): Use instance variables for this once the @name config
  # option is removed.
  private
  def identity
    @name || "#{@redis_url} #{@data_type}:#{@key}"
  end

  private
  def connect
    redis = Redis.new(
      :host => @host,
      :port => @port,
      :timeout => @timeout,
      :db => @db,
      :password => @password.nil? ? nil : @password.value
    )
    load_batch_script(redis) if @data_type == 'list' && (@batch_count > 1)
    return redis
  end # def connect

  private
  def load_batch_script(redis)
    #A Redis Lua EVAL script to fetch a count of keys
    #in case count is bigger than current items in queue whole queue will be returned without extra nil values
    redis_script = <<EOF
          local i = tonumber(ARGV[1])
          local res = {}
          local length = redis.call('llen',KEYS[1])
          if length < i then i = length end
          while (i > 0) do
            local item = redis.call("lpop", KEYS[1])
            if (not item) then
              break
            end
            table.insert(res, item)
            i = i-1
          end
          return res
EOF
    @redis_script_sha = redis.script(:load, redis_script)
  end

  private
  def queue_event(msg)
    begin
      @codec.decode(msg) do |event|
        decorate(event)
        event.publish
      end
    rescue => e # parse or event creation error
      @logger.error("Failed to create event", :message => msg, :exception => e,
                    :backtrace => e.backtrace);
    end
  end

  private
  def list_listener(redis)

    # blpop returns the 'key' read from as well as the item result
    # we only care about the result (2nd item in the list).
    item = redis.blpop(@key, 0)[1]

    # blpop failed or .. something?
    # TODO(sissel): handle the error
    return if item.nil?
    queue_event(item)

    # If @batch_count is 1, there's no need to continue.
    return if @batch_count == 1

    begin
      redis.evalsha(@redis_script_sha, [@key], [@batch_count-1]).each do |item|
        queue_event(item)
      end

      # Below is a commented-out implementation of 'batch fetch'
      # using pipelined LPOP calls. This in practice has been observed to
      # perform exactly the same in terms of event throughput as
      # the evalsha method. Given that the EVALSHA implementation uses
      # one call to Redis instead of N (where N == @batch_count) calls,
      # I decided to go with the 'evalsha' method of fetching N items
      # from Redis in bulk.
      #redis.pipelined do
        #error, item = redis.lpop(@key)
        #(@batch_count-1).times { redis.lpop(@key) }
      #end.each do |item|
        #queue_event(item) if item
      #end
      # --- End commented out implementation of 'batch fetch'
    rescue Redis::CommandError => e
      if e.to_s =~ /NOSCRIPT/ then
        @logger.warn("Redis may have been restarted, reloading Redis batch EVAL script", :exception => e);
        load_batch_script(redis)
        retry
      else
        raise e
      end
    end
  end

  private
  def channel_listener(redis)
    redis.subscribe @key do |on|
      on.subscribe do |channel, count|
        @logger.info("Subscribed", :channel => channel, :count => count)
      end

      on.message do |channel, message|
        queue_event message
      end

      on.unsubscribe do |channel, count|
        @logger.info("Unsubscribed", :channel => channel, :count => count)
      end
    end
  end

  private
  def pattern_channel_listener(redis)
    redis.psubscribe @key do |on|
      on.psubscribe do |channel, count|
        @logger.info("Subscribed", :channel => channel, :count => count)
      end

      on.pmessage do |ch, event, message|
        queue_event message
      end

      on.punsubscribe do |channel, count|
        @logger.info("Unsubscribed", :channel => channel, :count => count)
      end
    end
  end

  # Since both listeners have the same basic loop, we've abstracted the outer
  # loop.
  private
  def listener_loop(listener)
    while !finished?
      begin
        @redis ||= connect
        self.send listener, @redis
      rescue Redis::CannotConnectError => e
        @logger.warn("Redis connection problem", :exception => e)
        sleep 1
        @redis = connect
      rescue => e # Redis error
        @logger.warn("Failed to get event from Redis", :name => @name,
                     :exception => e, :backtrace => e.backtrace)
        raise e
      end
    end # while !finished?
  end # listener_loop

  public
  def run
    if @data_type == 'list'
      listener_loop :list_listener
    elsif @data_type == 'channel'
      listener_loop :channel_listener
    else
      listener_loop :pattern_channel_listener
    end
  end # def run

  public
  def teardown
    if @data_type == 'channel' and @redis
      @redis.unsubscribe
      @redis.quit
      @redis = nil
    end
    if @data_type == 'pattern_channel' and @redis
      @redis.punsubscribe
      @redis.quit
      @redis = nil
    end
  end
end # class LogStash::Inputs::Redis
