$:.push File.expand_path("../lib", __FILE__)

require 'fakeriak/version'

Gem::Specification.new do |s|
  s.name        = "fakeriak"
  s.version     = FakeRiak::Version::STRING
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Tapjoy"]
  s.email       = "eng@tapjoy.com"
  s.summary     = "An in-memory hash implementation of riak"
  s.description = "An in-memory hash implementation of riak"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = Dir["spec/**/*"]
  s.require_paths = ["lib"]
  
  s.add_dependency "execjs"
  s.add_dependency "multi_json"
  s.add_dependency "riak-client", ">= 1.0.0"

  s.add_development_dependency "rspec", ">= 1.0.0"
  s.add_development_dependency "appraisal", ">= 1.0.0"
end