# ActsAsShardable

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'acts_as_shardable'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install acts_as_shardable

## Usage

Example

In your model:

```ruby
class Mod2Model < ActiveRecord::Base
  acts_as_shardable by: :hash_id, mod: 2
end
```

and migrations:

```ruby
class CreateMod2Models < ActiveRecord::Migration
  def self.up
    shards.times do |i|
      create_table("mod2_models_%04d" % i) do |t|
        t.integer :hash_id
      end
    end
  end

  def self.down
    shards.times do |i|
      drop_table("cc_study_durations_%04d" % i)
    end
  end

  def self.shards
    2
  end
end
```

Then if you call

```ruby
Mod2Model.create(hash: 1)
```

it will save the record into `mod2_models_0001`.

And

```ruby
Mod2Model.create(hash: 2)
```

will save the record into `mod2_models_0002`.

## Development

After checking out the repo, run `bin/setup` to install dependencies. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

run `rake test` to run tests.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/scorix/acts_as_shardable. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

