require 'yaml'

module Configuration

  def load_configuration f
    YAML.load(File.read(f))
  end

end
