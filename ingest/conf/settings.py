class Settings:
    def __init__(self, main_path):
        self.main_path = main_path
        self.input_files_path = self.main_path + "/input_data"
        self.output_files_path = self.main_path + "/output_data"
        self.output_format = "csv"


def load_settings():
    main_path = "/Users/eric/spark-tutorial"
    return Settings(main_path)
