require "rubygems"
require "bundler/setup"

require 'ffi-rzmq'
require 'celluloid'
require 'json'
require 'date'

class App
  attr_accessor :options
  attr_accessor :zmq_context

  @@the_app = nil
  def self.app
    @@the_app = App.new unless @@the_app
    @@the_app
  end

  def initialize
    @options = {
      event_source_address: '127.0.0.1',
      zmq_publish_socket_port: 38200
    }
    @zmq_context = ZMQ::Context.create(1)
    puts "libzmq version #{ZMQ::LibZMQ.version}"
  end

  def run
    puts "app.run"
    EventLogger.supervise_as :event_logger
    EventReader.supervise_as :event_reader
    EventWriter.supervise_as :event_writer
    (1..4).each do |i|
      symbol = :"event_source_#{i}"
      EventSource.supervise_as symbol,symbol.to_s,i
    end
    sleep
  end
end
class EventSource
  include Celluloid
  attr_reader :delay
  attr_reader :index
  attr_reader :name
  attr_reader :timer
  def initialize(name,delay)
    @name = name
    @delay = delay
    @index = 0
    puts "EventSource #{@name} starting"
    @timer = after(@delay) { gen_event }
  end
  def gen_event
    @index += 1
    event = {time: DateTime.now.to_s, name: @name, index: @index}
    puts event.to_json
    Celluloid::Actor[:event_writer].async.write event.to_json
    @timer.reset
  end
end
class EventWriter
  include Celluloid
  attr_reader :pub_socket

  def initialize
    @pub_socket = App.app.zmq_context.socket ZMQ::PUB
    @pub_socket.bind("tcp://#{App.app.options[:event_source_address]}:#{App.app.options[:zmq_publish_socket_port]}")
    @pub_socket.setsockopt(ZMQ::HWM, 64*1024)
    puts "EventWriter starting"
  end
  def write(event)
    @pub_socket.send_string event
  end
end
class EventReader
  include Celluloid
  attr_reader :sub_socket
  def initialize
    @sub_socket = App.app.zmq_context.socket ZMQ::SUB
    @sub_socket.setsockopt(ZMQ::SUBSCRIBE,"")
    @sub_socket.connect("tcp://#{App.app.options[:event_source_address]}:#{App.app.options[:zmq_publish_socket_port]}")
    puts "EventReader starting"
    async.run
  end
  def run
    loop do
      msg = ""
      stat = sub_socket.recv_string msg
      if stat == 0
        Celluloid::Actor[:event_logger].async.log msg
      else
        puts "No recv_string"
      end
    end
  end
end
class EventLogger
  include Celluloid
  attr_reader :log_count
  attr_reader :logfile
  def initialize
    @log_count = 0
    @logfile = File.new('zmq_log.txt','a+')
    puts "EventLogger starting"
    @logfile.puts "EventLogger: starting at #{DateTime.now.to_s}"
    @logfile.fsync
  end
  def log msg
    @log_count += 1
    log_text = "#{@log_count}: #{msg}"
    puts log_text
    @logfile.puts log_text
    @logfile.fsync
  end
end

App.app.run

