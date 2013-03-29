Gem::Specification.new do |s|
  s.name               = "tuhura-ingestion"
  s.version            = "0.0.1"
  s.default_executable = "tuhura-ingestion"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Chief Max"]
  s.date = '2013-03-06'
  s.description = %q{This gem provides tools to ingest stuff into the Tuhura universe}
  s.email = %q{max@incoming-media.com}
  
  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  
  s.rubygems_version = %q{1.6.2}
  s.summary = %q{tuhura-ingestion!}

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
    else
    end
  else
  end
end
