#
# An app which just burns cycles
#

require 'optparse'

$count = 20
$delay = 1.0

op = OptionParser.new
descr = "Run in circles:\n\n"
op.banner = "Usage: #{op.program_name} [options]\n\nRun in circles!\n"
op.on('-c', "--count COUNT", "Loop N time [#{$count}]") {|c| $count = c.to_i }
op.on('-d', "--delay SECONDS", "Delay in each loop [#{$delay}]") {|d| $delay = d.to_f }
op.on('', "--oml-collect URL", "Ignore OML") 
op.on_tail('-h', "--help", "Show this message") { $stderr.puts op; exit }
op.parse(ARGV)

puts "START count: #{$count} delay: #{$delay}"
while ($count -= 1) > 0
  sleep $delay if $delay > 0
end
puts "DONE"
