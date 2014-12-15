require 'spec_helper'
require 'riak/client/memory_backend'
require 'riak/version'

describe Riak::Client::MemoryBackend do
  let(:client) do
    if ENV['LIVE']
      Riak::Client.new
    else
      Riak::Client.new(:protobuffs_backend => :Memory)
    end
  end
  let(:subject) do
    subject = nil
    client.backend {|backend| subject = backend}
    subject
  end
  let(:bucket) do
    client.bucket('fakeriak')
  end

  before(:each) do
    bucket.keys.each {|key| bucket.delete(key)}
  end

  describe 'ping' do
    it 'should be true' do
      expect(client.ping).to eq(true)
    end
  end

  describe 'get_client_id' do
    it 'should be nulled out by default' do
      expect(client.client_id).to eq("\x00\x00\x00\x00")
    end
  end
  
  describe 'set_client_id' do
    it 'should update the client id' do
      client.client_id = 'abc123'
      expect(client.client_id).to eq('abc123')
    end
  end
  
  describe 'server_info' do
    it 'should return the node' do
      expect(subject.server_info[:node]).to eq('riak@127.0.0.1')
    end

    it 'should return the server version' do
      expect(subject.server_info[:node]).not_to be_nil
    end
  end
  
  if Riak::VERSION < '2.0.0'
    describe 'stats' do
      it 'should return stats' do
        expect(subject.stats).not_to be_nil
      end
    end
  end
  
  describe 'get_object' do
    it 'should raise an error if not existent' do
      expect { client.get_object('fakeriak', 'fakekey') }.to raise_error(Riak::ProtobuffsFailedRequest)
    end

    it 'should get the value if exists' do
      object = bucket.get_or_new('realkey')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'
      object.store

      object = client.get_object('fakeriak', 'realkey')
      expect(object.raw_data).to eq('Hello world')
    end
  end
  
  describe 'store_object' do
    it 'build the object' do
      object = bucket.get_or_new('realkey')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'

      result = object.store
      expect(result).to eq(object)
    end
  end
  
  describe 'delete_object' do
    before(:each) do
      object = bucket.get_or_new('realkey')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'
      object.store
    end

    it 'should be true if nonexistent' do
      result = client.delete_object('fakeriak', 'fakekey')
      expect(result).to eq(true)
    end

    it 'should be true if exists' do
      result = client.delete_object('fakeriak', 'realkey')
      expect(result).to eq(true)
      expect { client.get_object('fakeriak', 'realkey') }.to raise_error(Riak::ProtobuffsFailedRequest)
    end
  end
  
  describe 'get_counter' do
    before(:each) do
      bucket.allow_mult = true
    end

    it 'should be 0 if nonexistent' do
      counter = bucket.counter('users')
      expect(counter.value).to eq(0)
    end

    it 'should get current value if exists' do
      counter = bucket.counter('users')
      counter.increment

      counter = bucket.counter('users')
      expect(counter.value).to eq(1)
    end
  end
  
  describe 'post_counter' do
    before(:each) do
      bucket.allow_mult = true
    end

    it 'should return nil' do
      counter = bucket.counter('users')
      result = counter.increment

      expect(result).to eq(nil)
    end
  end
  
  describe 'get_bucket_props' do
    it 'should return default props' do
      expect(bucket.props).to have_key("r")
    end
  end
  
  describe 'set_bucket_props' do
    it 'should update props' do
      bucket.allow_mult = true
      expect(bucket.props).to have_key("allow_mult")
    end
  end
  
  describe 'clear_bucket_props' do
    it 'should clear new props' do
      bucket.allow_mult = true
      bucket.clear_props
      expect(bucket.props).not_to have_key("allow_mult")
    end
  end
  
  describe 'list_keys' do
    it 'should be empty by default' do
      expect(bucket.keys).to eq([])
    end

    it 'should include created keys' do
      object = bucket.get_or_new('realkey')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'
      object.store

      expect(bucket.keys).to eq(['realkey'])
    end
  end
  
  describe 'list_buckets' do
    it 'should not include previous buckets by default' do
      bucket = client.buckets.detect {|bucket| bucket.name == 'fakeriak'}
      expect(bucket).to eq(nil)
    end

    it 'should not new buckets' do
      object = bucket.get_or_new('realkey')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'
      object.store

      bucket = client.buckets.detect {|bucket| bucket.name == 'fakeriak'}
      expect(bucket).not_to eq(nil)
    end
  end
  
  describe 'mapred' do
    it 'should process' do
      object = bucket.get_or_new('key1')
      object.raw_data = 'Hello world'
      object.content_type = 'text/html'
      object.store

      object = bucket.get_or_new('key2')
      object.raw_data = 'Hello world 2'
      object.content_type = 'text/html'
      object.store

      script = <<-eos
        function(obj) {
          return [obj.values[0].data];
        }
eos
      mapred = Riak::MapReduce.new(client).add(bucket.name).map(script, :keep => true).run
      expect(mapred).to eq(['Hello world', 'Hello world 2'])
    end
  end
  
  describe 'get_index' do
    it 'should fail when index is nonexistent' do
      expect { client.get_search_index('invalid') }.to raise_error(Riak::ProtobuffsErrorResponse)
    end

    it 'should return index when existent' do
      client.create_search_index('index1')
      search_index = client.get_search_index('index1')

      expect(search_index).to be_instance_of(Riak::Client::BeefcakeProtobuffsBackend::RpbYokozunaIndex)
    end
  end
  
  describe 'create_search_index' do
    it 'should set attributes on index' do
      search_index = client.get_search_index('index1')
      expect(search_index.name).to eq('index1')
      expect(search_index.schema).to eq('_yz_default')
      expect(search_index.n_val).to eq(3)
    end
  end
  
  describe 'get_search_index' do
    # TODO
  end
  
  describe 'update_search_index' do
    # TODO
  end
  
  describe 'delete_search_index' do
    # TODO
  end
  
  describe 'create_search_schema' do
    # TODO
  end
  
  describe 'get_search_schema' do
    # TODO
  end
  
  describe 'teardown' do
    it 'should no-op' do
      result = subject.teardown
      expect(result).to eq(nil)
    end
  end
  
  unless ENV['LIVE']
    describe 'search' do
      it 'should raise an error' do
        expect { subject.search('test', 'a=1') }.to raise_error(NotImplementedError)
      end
    end
    
    describe 'link_walk' do
      it 'should raise an error' do
        expect { subject.link_walk('test1', 'test2') }.to raise_error(NotImplementedError)
      end
    end
    
    describe 'get_file' do
      it 'should raise an error' do
        expect { subject.get_file('test') }.to raise_error(NotImplementedError)
      end
    end
    
    describe 'file_exists?' do
      it 'should raise an error' do
        expect { subject.file_exists?('test') }.to raise_error(NotImplementedError)
      end
    end
    
    describe 'delete_file' do
      it 'should raise an error' do
        expect { subject.delete_file('test') }.to raise_error(NotImplementedError)
      end
    end
    
    describe 'store_file' do
      it 'should raise an error' do
        expect { subject.store_file('test') }.to raise_error(NotImplementedError)
      end
    end
  end
end