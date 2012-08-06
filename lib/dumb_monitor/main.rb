require 'dumb_monitor'

require 'time' # iso8601
require 'pp'

require 'dumbstats'
require 'dumbstats/stats'

module DumbMonitor
class Main
  attr_accessor :opts, :host, :running, :exception
  attr_accessor :sample_rate, :interval, :dt, :now
  attr_accessor :status_file, :status_dir
  attr_accessor :client
  attr_accessor :stats # global stats
  attr_accessor :service_stats # Dumbstats::Stats
  attr_accessor :service_stats_prev

  def initialize
    self.stats = Dumbstats::Stats.new(:name => stats_name)
  end

  def stats_name
    self.class.name.to_sym
  end

  def parse_opts! a = nil
    a ||= ARGV.dup
    self.opts = { }
    while v = a.shift
      k, v = v.split('=', 2)
      opts[k.to_sym] = v
    end
    self.sample_rate = (opts[:sample_rate] || 1).to_f
    self.interval = (opts[:interval] || 60).to_i
    self
  end

  def host
    raise "Subclass Responsibility"
  end

  def run_monitor!
    @stats_last_delivery = Time.now.utc

    self.running = true
    Signal.trap("TERM") do
      self.running = false
      exit 1
    end

    self.status_dir = opts[:status_dir] || '.'
    self.status_file = opts[:status_file] || "#{status_dir}/#{host[:host]}:#{host[:port]}.status"

    while @running
      begin
        data = get_stats!
        self.now = Time.now.utc
        stats.count! :samples
        write_status_file! data
        collect_stats! data
        deliver_stats!
      rescue ::Exception => exc
        $stderr.puts "#{$0}: ERROR in run_monitor!: #{exc.inspect}\n  #{exc.backtrace * "\n  "}"
        self.exception = exc
        self.running = false
      end
      sleep sample
    end

    raise self.exception if self.exception

    $stdout.puts "DONE!"
    self
  end

  def connect_client!
    return client if client
    host = self.host
    self.client = TCPSocket.new(host[:host], host[:port])
  rescue ::Exception => exc
    $stderr.puts "#{$0}: #{self}: ERROR in connect_client! #{host.inspect}: #{exc.inspect}\n  #{exc.backtrace * "\n  "}"
    sleep 10
    retry
  end

  def get_stats!
    raise "Subclass Responsibility"
  end

  # Dump raw status to file.
  def write_status_file! data
    File.open(tmp = "#{status_file}.tmp", "w+") do | out |
      out.puts "Time: #{now.iso8601(4)}"
      out.puts "Host: #{host[:host]}"
      out.puts "Port: #{host[:port]}"
      out.puts data
    end
    File.chmod(0666, tmp) rescue nil
    File.rename(tmp, status_file) # atomic FS operation.
    $stderr.write '.' if opts[:tick]
    self
  end

  def deliver_stats!
    if (@dt = now - @stats_last_delivery) >= interval
      $stderr.write "+" if opts[:tick]
      @stats_last_delivery = now

      # Convert counters to rates.
      stats[:samples].rate!(dt)
      serivce_stats.each do | k, b |
        b.rate! dt if k != :size
      end
      service_stats.finish!

      if opts[:graphite_host]
        send_to_graphite!
      end
      if opts[:dump_stats]
        dump_stats!
      end

      # Empty stats
      service_stats.clear!
      stats.clear!
    end
    self
  end

  def send_to_graphite!
    g = graphite
    g.add_bucket! stats[:samples]
    service_stats.each do | k, b |
      g.add_bucket!(b,
        :now => now,
        :ignore => IGNORE
        )
    end
    self
  end
  IGNORE = [ :count, :median, :sum ].freeze

  # Returns a configured Dumbstats::Graphite instance, running in its own Thread.
  def graphite
    return @graphite if @graphite
    require 'dumbstats/graphite'
    g =
      Dumbstats::Graphite.new(
                          :output_io => opts[:testing] && $stderr, # DEBUG!!
                          :host => opts[:graphite_host],
                          :port => opts[:graphite_port]
                          )
    g.prefix = "#{opts[:graphite_prefix]}stomp.#{graphite_path_host g}.#{host[:port]}."
    if log = opts[:graphite_log]
      $stderr.puts "  #{$$} opening #{log.inspect}"
      g.log_io = File.open(log, "w+")
      File.chmod(0666, log) rescue nil
    end
    Thread.new do
      $stderr.puts "  #{$$} created #{g} in #{Thread.current}"
      g.run!
    end
    @graphite = g
  end

  def graphite_path_host g
    @graphite_path_host ||=
      g.encode_path(opts[:graphite_path_host] || host[:host].sub(/\.[^.]+\.[^.]+$/, '')).freeze
  end

  def dump_stats! out = nil
    out ||= $stdout
    out.puts "============================================"
    service_stats.each do | k, b |
      out.write "   #{k}: "
      b.to_a.each do | n, v |
        out.write "#{n}=#{v} "
      end
      out.write "\n"
    end
    self
  end

  def collect_stats! data
    self.service_stats_prev ||= Dumbstats::Stats.new
    h = parse_stats! data
    pp h
    raise "UNIMPLEMENTED"
    self
  end

  def parse_stats! lines
    raise "Subclass Responsibility"
  end

  def run!
    parse_opts!
    connect_client!
    run_monitor!
  rescue ::Exception => exc
    STDERR.puts "#{$0}: #{self}: ERROR #{exc.inspect}\n  #{exc.backtrace * "\n  "}"
    raise
  end
end # end
end # module

