$debugger = false
if ENV['RUBY_DEBUG']
  case RUBY_VERSION
  when /^1\.9/
    gem 'debugger'
    require 'debugger'
  else
    gem 'ruby-debug'
    require 'ruby-debug'
  end
  $debugger = true
end

require 'dumb_monitor'
require 'dumb_monitor/logging'

require 'time' # iso8601
require 'pp'

require 'dumbstats'
require 'dumbstats/stats'
require 'dumb_monitor/logging'

module DumbMonitor
class Main
  include Logging
  attr_accessor :opts, :host, :running, :exception
  attr_accessor :sample_interval, :interval, :dt, :now
  attr_accessor :status_file, :status_dir
  attr_accessor :client
  attr_accessor :stats # Global stats collected over :interval
  attr_accessor :service_stats # Service stats collected over :interval
  attr_accessor :service_sample # Hash of raw values, sampled via parse_stats!
  attr_accessor :service_sample_prev # Hash merged from service_sample until send_stats!

  def initialize
    self.stats = Dumbstats::Stats.new(:name => stats_name)
    self.sample_interval = 1
    self.interval = 60
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
    if x = opts[:sample_interval]
      self.sample_interval = x.to_f
    end
    if x = opts[:interval]
      self.interval = x.to_i
    end
    self
  end

  def host
    raise "Subclass Responsibility"
  end

  def run_monitor!
    before_run!
    run_monitor_loop!
    raise self.exception if self.exception
    _log "run_monitor!: DONE!"
    self
  ensure
    after_run!
  end

  def run_monitor_loop!
    while @running
      begin
        sample!
      rescue ::Exception => exc
        _log exc
        self.exception = exc
        self.running = false
      end
      sleep sample_interval
    end

    self
  end

  def before_run!
    self.running = true
    @save_TERM = Signal.trap("TERM") do
      self.running = false
      exit 1
    end

    self.status_dir = opts[:status_dir] || '.'
    self.status_file = opts[:status_file] || "#{status_dir}/#{host[:host]}:#{host[:port]}.status"
    self.service_stats = Dumbstats::Stats.new
    self
  end

  def after_run!
    Signal.trap("TERM", @save_TERM)
    self
  end

  def sample! data = nil
    @stats_last_delivery ||= Time.now.utc
    data = get_stats! unless data
    self.now = Time.now.utc
    stats.count! :samples
    write_status_file! data
    collect_stats! data
    dump_raw!(service_sample) if opts[:dump_sample]
    check_stats_interval!
    self
  end

  def connect_client!
    return client if client
    host = self.host
    _connect_client!
    _log "Connected to #{host.inspect}"
  rescue ::SystemExit, ::Interrupt, ::SignalException
    raise
  rescue ::Exception => exc
    _log exc
    sleep 10
    retry
  end

  def _connect_client!
    self.client = TCPSocket.new(host[:host], host[:port])
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

  def collect_stats! data
    self.service_sample_prev ||= { }
    self.service_sample = parse_stats! data
    service_sample.each do | k, v |
      next if ignore_stats_keys.include?(k)
      v = v.to_i
      if count_stats_keys.include?(k)
        # $stderr.puts "  #{k.inspect} #{service_sample_prev[k].inspect} => #{v.inspect}"
        service_stats.add_delta! k, service_sample_prev[k], v
        service_sample_prev[k] = v
      else
        service_stats.add! k, v
      end
    end
    self
  end

  # Subclass override.
  def ignore_stats_keys; EMPTY_Array; end
  # Subclass override.
  def count_stats_keys; EMPTY_Array; end

  def parse_stats! lines
    raise "Subclass Responsibility"
  end


  def check_stats_interval!
    if (@dt = now - @stats_last_delivery) >= interval
      deliver_stats!
    end
    self
  end

    def deliver_stats!
      before_deliver_stats!
      $stderr.write "+" if opts[:tick]
      @stats_last_delivery = now

      stats[:samples].rate!(dt)
      prepare_stats!
      stats.finish!

      send_to_graphite! if opts[:graphite_host] || opts[:testing]
      dump_stats! if opts[:dump_stats]

      # Empty stats
      service_stats.clear!
      stats.clear!
      after_deliver_stats!
      self
    end

    def prepare_stats!
      # Convert counters to rates.
      service_stats.each do | k, b |
        b.rate!(dt) if count_stats_keys.include?(k)
      end
      service_stats.finish!
      self
    end

    def before_deliver_stats!
      self
    end

    def after_deliver_stats!
      self
    end

  def send_to_graphite!
    g = graphite
    stats.each do | k, b |
      g.add_bucket! b, :now => now, :ignore => ignore_bucket_items
    end
    service_stats.each do | k, b |
      g.add_bucket!(b,
        :now => now,
        :ignore => ignore_bucket_items
        )
    end
    self
  end

  # Subclass override.
  def ignore_bucket_items; EMPTY_Array; end

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
    g.prefix = "#{opts[:graphite_prefix]}#{graphite_prefix}#{graphite_path_host g}.#{host[:port]}."
    if log = opts[:graphite_log]
      _log "opening #{log.inspect}"
      g.log_io = File.open(log, "w+")
      File.chmod(0666, log) rescue nil
    end
    Thread.new do
      _log "created #{g} in #{Thread.current}"
      g.run!
    end
    @graphite = g
  end

  # Subclass override.
  def graphite_prefix; EMPTY_String; end

  def graphite_path_host g
    @graphite_path_host ||=
      (
      if h = host[:host] and h !~ /^[.0-9]+$/
        # Trim TLD off hostname.
        h = h.sub(/\.[^.]+\.[^.]+$/, '')
      end
      g.encode_path(opts[:graphite_path_host] || h)
      ).freeze
  end

  def dump_raw! data, out = nil
    out ||= $stdout
    out.puts "============================================"
    PP.pp(data, out)
    out.flush
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

  def run!
    parse_opts!
    connect_client!
    run_monitor!
  rescue ::Exception => exc
    _log exc
    raise
  end

end # class
end # module

