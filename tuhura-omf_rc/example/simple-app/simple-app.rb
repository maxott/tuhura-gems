#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
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
