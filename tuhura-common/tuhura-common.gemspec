Gem::Specification.new do |s|
  s.name               = "tuhura-common"
  s.version            = "0.0.1"
  s.default_executable = "tuhura-common"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Chief Max"]
  s.date = '2013-03-06'
  s.description = %q{A collection of common capabilites used throughout the Tuhura environment}
  s.email = %q{max@incoming-media.com}
  
  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  
  s.rubygems_version = %q{1.6.2}
  s.summary = %q{tuhura-common!}
  
  s.add_runtime_dependency 'kafka-rb', "~> 0.0"
  s.add_runtime_dependency 'hbase-jruby', "~> 0.2"
  s.add_runtime_dependency 'log4jruby', "~> 0.5"
  s.add_runtime_dependency 'zookeeper', "~> 1.4"

end
