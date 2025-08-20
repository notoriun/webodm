def create_default_odm_task_options(env_options: str):
    options = {}

    options_list = env_options.split(",")
    options_list_with_values = (option.split("=") for option in options_list)

    for option, value in options_list_with_values:
        options[option] = value

    return options
