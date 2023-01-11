################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import orquestra.sdk as sdk

# >> Tutorial code snippet: save config

config = sdk.RuntimeConfig.qe(uri="https://prod-d.orquestra.io", token="my_token")
print(config)

# >> End save config


# >> End save config


del config

# >> Tutorial code snippet: list configs

print(sdk.RuntimeConfig.list_configs())
# If 'prod-d' is the only configuration that has been saved, this will display
# ['prod-d'] together with some built-ins configurations, like ray or in_process

# >> End list configs


# >> Tutorial code snippet: load config

loaded_config = sdk.RuntimeConfig.load("prod-d")
print(loaded_config)

# >> End load config
