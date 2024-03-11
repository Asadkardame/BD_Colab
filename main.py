from incrementalLoad import CarInsuranceClaim

if __name__ == "__main__":
    processor = CarInsuranceClaim()
    processor.initialize_spark()
    processor.process_data()
    processor.close_spark()