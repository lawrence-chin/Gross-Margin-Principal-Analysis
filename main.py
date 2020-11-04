import extract 
import transform
import sys
import traceback
import load

def main(begin_date, end_date):

    extract_dict = extract.get_extracts(begin_date, end_date)

    output_dict = transform.get_transforms(extract_dict)

    load.load_data(output_dict)


if __name__ == "__main__":
    begin_date = sys.argv[1]
    end_date = sys.argv[2]
    print('Begin Date: ' + str(begin_date))
    print('End Date: ' + str(end_date))

    try:
        main(begin_date, end_date)
    except Exception as e:
        print(e)
        print(traceback.format_exc())