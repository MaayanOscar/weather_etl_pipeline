import os
import yaml


def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    # with open(config_path, "r") as f:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


if __name__ == '__main__':
    load_config()

# bucket = config["s3"]["bucket_name"]
# base_url = config["api"]["base_url"]