import shutil
import argparse
import apache_beam as beam

from apache_beam.runners.direct import DirectRunner

def move_file(source_path, target_path):
    try:
        shutil.move(source_path, target_path)
        print(f"Moved file from {source_path} to {target_path}")
    except FileNotFoundError:
        print(f"Source file not found {source_path}")
    except PermissionError:
        print(f"Permission denied {source_path}")
    except Exception as e:
        print(f"Error moving file: {e}")

def main():
    pipeline_options = beam.options.pipeline_options.PipelineOptions()
    pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DirectRunner'
    
    parser = argparse.ArgumentParser(description="Move files from source to target")
    parser.add_argument("--data_source", required=True, help="Source file path")
    parser.add_argument("--data_target", required=True, help="Target file path")
    args = parser.parse_args()

    with beam.Pipeline(options=pipeline_options) as p:
        files_to_move = p | "Moving files" >> beam.Create([(args.data_source, args.data_target)])

        class MoveFiles(beam.DoFn):
            def process(self, element):
                source_path, target_path = element
                move_file(source_path, target_path)
                yield None

        _ = files_to_move | beam.ParDo(MoveFiles())

if __name__ == "__main__":
    main()
