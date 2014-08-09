# encoding: utf-8
require "logstash/namespace"

# A 'fake' queue that publishes an event to the Pipeline
# any time an event is pushed to it.
# This is meant for backwards compatibility with inputs
# that previously pushed messages onto a Queue. 
class LogStash::EventPublishingQueue
  
  def push(event)
    event.publish
  end
  
  alias :<< :push
end
