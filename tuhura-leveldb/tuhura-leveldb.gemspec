Gem::Specification.new do |s|
  s.name               = "tuhura-leveldb"
  s.version            = "0.0.1"
  s.default_executable = "tuhura-leveldb"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Chief Max"]
  s.date = '2013-03-06'
  s.description = %q{An implementation of the Tuura DB interface using LevelDB}
  s.email = %q{max@incoming-media.com}

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.rubygems_version = %q{1.6.2}
  s.summary = %q{tuhura-leveldb!}

  s.add_runtime_dependency 'leveldb' #, "~> 0.0"


end
