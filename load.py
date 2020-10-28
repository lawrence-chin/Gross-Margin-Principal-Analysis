import settings
from datetime import datetime as dt

def load_data(output_dict):
    filepath = settings.output_file_path
    deal_data = output_dict['deal_data'].copy()
    principal_data = output_dict['principal_data'].copy()
    principal_max_data = output_dict['principal_mix_data'].copy()

    today = str(dt.now().date())

    deal_data.to_csv(filepath + 'GMA_Deal_Data_' + today + '.csv')

    principal_data.to_csv(filepath + 'GMA_Principal_Data_' + today + '.csv')

    principal_max_data.to_csv(filepath + 'GMA_Cohort_Data_' + today + '.csv')

    print('All Outputted!')