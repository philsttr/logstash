require "test_utils"

describe "inputs/generator" do
  extend LogStash::RSpec

  context "performance", :performance => true do
    event_count = 100000 + rand(50000)

    config <<-CONFIG
      input {
        generator {
          type => "blah"
          count => #{event_count}
        }
      }
    CONFIG

    input do |pipeline, queue|
      start = Time.now
      event_count.times do |i|
        event = queue.pop
        insist { event["sequence"] } == i
      end
      duration = Time.now - start
      puts "inputs/generator rate: #{"%02.0f/sec" % (event_count / duration)}, elapsed: #{duration}s"
    end # input
  end

  context "generate configured message" do
    config <<-CONFIG
      input {
        generator {
          count => 2
          message => "foo"
        }
      }
    CONFIG

    input do |pipeline, queue|
      event = queue.pop
      insist { event["sequence"] } == 0
      insist { event["message"] } == "foo"

      event = queue.pop
      insist { event["sequence"] } == 1
      insist { event["message"] } == "foo"

      insist { queue.size } == 0
    end # input

    context "generate message from stdin" do
      config <<-CONFIG
        input {
          generator {
            count => 2
            message => "stdin"
          }
        }
      CONFIG

      input false do |pipeline, queue|
        saved_stdin = $stdin
        stdin_mock = StringIO.new
        $stdin = stdin_mock
        stdin_mock.should_receive(:readline).once.and_return("bar")

        pipeline_thread = Thread.new { pipeline.run }
        sleep 0.1 while !pipeline.ready?

        event = queue.pop
        insist { event["sequence"] } == 0
        insist { event["message"] } == "bar"

        event = queue.pop
        insist { event["sequence"] } == 1
        insist { event["message"] } == "bar"

        insist { queue.size } == 0
          
        pipeline.shutdown
        pipeline_thread.join
        $stdin = saved_stdin
      end # input
      
    end
  end
end
