class LogStash::Pipeline::OutputEventHandler
  java_implements com.lmax.disruptor.EventHandler
  
  def initialize(pipeline)
    @pipeline = pipeline
  end
  
  java_signature 'void onEvent(T event, long sequence, boolean endOfBatch)'
  def onEvent(event, sequence, end_of_batch)
    if (!@threadNameSet)
      LogStash::Util::set_thread_name(">output")
      @threadNameSet = true
    end
    if event == LogStash::ShutdownSignal || event.cancelled?
      return
    end
    @pipeline.output(event, end_of_batch)
  end

end
