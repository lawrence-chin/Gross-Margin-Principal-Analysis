import extract 
import transform
import sys

def main(begin_date, end_date):

    extract_dict = extract.get_extracts(begin_date, end_date)

    output_dict = transform.get_transforms()

    load.output_data(output_dict)


if __name__ == "__main__":
    begin_date = sys.argv[0]
    end_date = sys.argv[1]

    try:
        main(begin_date, end_date)
    except Exception as e:
        print(e)