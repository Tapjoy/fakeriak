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
  
  s.add_dependency "riak"

  s.add_development_dependency "rspec"
end