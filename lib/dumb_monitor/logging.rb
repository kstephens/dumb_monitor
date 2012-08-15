require 'dumb_monitor'

module DumbMonitor
  module Logging
    def _log msg = nil, more = nil
      msg ||= yield if block_given?
      case msg
      when ::Exception
        msg = "ERROR: #{msg.inspect}\n  #{msg.backtrace * "\n  "}"
      end
      more << ' ' if more
      $stderr.puts "#{Time.now.utc.iso8601(3)} #{$$} #{Thread.current} #{self} #{more}#{msg}"
    end
  end
end
