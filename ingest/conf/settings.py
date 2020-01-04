class Settings:
    def __init__(self, main_path):
        self.main_path = main_path
        self.input_file_path = self.main_path + "input_files"
        self.output_file_path = self.main_path + "output_files"
        self.output_format = "csv"


def load_dev_settings():
    main_path = "/Users/eric/spark-tutorial"
    return Settings(main_path)
