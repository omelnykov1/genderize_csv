from genderize import Genderize, GenderizeException
import csv
import sys
import os.path
import time
import argparse
import logging

import jpyhelper as jpyh

# helper function
def get_gender(gender):
  binary_genders = ""
  if gender == None:
    binary_genders = ['0', '0']
  elif gender == 'female':
    binary_genders = ['1', '0']
  else:
    binary_genders = ['0', '1']

  return binary_genders

def genderize(args):
  print(args)
  #File initialization
  dir_path = os.path.dirname(os.path.realpath(__file__))

  logging.basicConfig(filename=dir_path + os.sep + "log.txt", level=logging.DEBUG,
                      format='%(asctime)s %(levelname)s %(name)s %(message)s')
  logger=logging.getLogger(__name__)

  ofilename, ofile_extension = os.path.splitext(args.output)

  ofile = ofilename + "_override" + ".csv" if args.override else ofilename + "_no_override" + ".csv"
  ifile = args.input

  if os.path.isabs(ifile):
    print("\n--- Input file: " + ifile)
  else:
    print("\n--- Input file: " + dir_path + os.sep + ifile)

  if os.path.isabs(ofile):
    print("--- Output file: " + ofile)
  else:
    print("--- Output file: " + dir_path + os.sep + ofile + "\n")

    #File integrity checking
    if not os.path.exists(ifile):
        print("--- Input file does not exist. Exiting.\n")
        sys.exit()

    if not os.path.exists(os.path.dirname(ofile)):
        print("--- Error! Invalid output file path. Exiting.\n")
        sys.exit()

    #Some set up stuff
    ##csv.field_size_limit(sys.maxsize)

    #Initialize API key
    if not args.key == "NO_API":
        print("--- API key: " + args.key + "\n")
        genderize = Genderize(
            user_agent='GenderizeDocs/0.0',
            api_key=args.key)
        key_present = True
    else:
        print("--- No API key provided.\n")
        key_present = False

    #Open ifile
    with open(ifile, 'r', encoding="utf8") as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',', skipinitialspace=True)
        # grabbing csv headers
        fieldnames = next(readCSV)
        # initializing default values dict
        prev_data = dict()
        first_name = []
        # holding to user_ids to use them later in order to access previous data
        user_ids = []
        for row in readCSV: #Read CSV into first_name list
            # getting rid of extra empty spaces in names
            name = row[1].strip()
            user_id = row[0]
            # assigning default data to the name key
            prev_data[user_id] = row
            user_ids.append(user_id)
            first_name.append(name)
        

        if args.auto == True:
            uniq_first_name = list(set(first_name))
            chunks = list(jpyh.splitlist(uniq_first_name, 10))
            print("--- Read CSV with " + str(len(first_name)) + " first_name. " + str(len(uniq_first_name)) + " unique.")
        else:
            chunks = list(jpyh.splitlist(first_name, 10))
            print("--- Read CSV with " + str(len(first_name)) + " first_name")

        print("--- Processed into " + str(len(chunks)) + " chunks")

        if jpyh.query_yes_no("\n---! Ready to send to Genderdize. Proceed?") == False:
            print("Exiting...\n")
            sys.exit()

        if os.path.isfile(ofile):
            if jpyh.query_yes_no("---! Output file exists, overwrite?") == False:
                print("Exiting...\n")
                sys.exit()
            print("\n")

        if args.auto == True:
            ofile = ofile + ".tmp"
        

        response_time = []
        gender_responses = list()
        with open(ofile, 'w', newline='', encoding="utf8") as f:
            writer = csv.writer(f)
            row = list([*fieldnames, "female", "male"]) if args.override else [*fieldnames, "gender", "probability", "count"]
            writer.writerow(list(row))
            chunks_len = len(chunks)
            stopped = False
            for index, chunk in enumerate(chunks):
                if stopped:
                    break
                success = False
                while not success:
                    try:
                        start = time.time()

                        if key_present:
                            dataset = genderize.get(chunk)
                        else:
                            dataset = Genderize().get(chunk)
                        gender_responses.append(dataset)
                        success = True
                    except GenderizeException as e:
                        #print("\n" + str(e))
                        logger.error(e)

                        #Error handling
                        if "response not in JSON format" in str(e) and args.catch == True:
                            if jpyh.query_yes_no("\n---!! 502 detected, try again?") == True:
                                success = False
                                continue
                        elif "Invalid API key" in str(e) and args.catch == True:
                            print("\n---!! Error, invalid API key! Check log file for details.\n")
                        else:
                            print("\n---!! GenderizeException - You probably exceeded the request limit, please add or purchase a API key. Check log file for details.\n")
                        stopped = True
                        break

                    response_time.append(time.time() - start)
                    print("Processed chunk " + str(index + 1) + " of " + str(chunks_len) + " -- Time remaining (est.): " + \
                        str( round( (sum(response_time) / len(response_time) * (chunks_len - index - 1)), 3)) + "s")
                    for data in dataset:
                        new_data = []
                        user_id = user_ids.pop(0)
                        # print(name)
                        if args.override:
                            gender = data["gender"]
                            binary_genders = get_gender(gender)
                            new_data = [*prev_data[user_id], *binary_genders]
                        else:
                            # values that has been returned by genderize service
                            val = list(data.values())
                            # removing name key value pairs, since my default value already contains name
                            val.pop(0)
                            new_data = [*prev_data[user_id], *val]
                        
                        writer.writerow(new_data)
                    break
                

            if args.auto == True:
                print("\nCompleting identical first_name...\n")
                #AUTOCOMPLETE first_name
                #Create master dict
                gender_dict = dict()
                for response in gender_responses:
                    for d in response:
                        gender_dict[d.get("name")] = [d.get("gender"), d.get("probability"), d.get("count")]

                filename, file_extension = os.path.splitext(ofile)
                with open(filename, 'w', newline='', encoding="utf8") as f:
                    writer = csv.writer(f)
                    row = list([*fieldnames, "female", "male"]) if args.override else [*fieldnames, "gender", "probability", "count"]
                    writer.writerow(list(row))
                    for name in first_name:
                        data = gender_dict.get(name)
                        for key in prev_data:
                            prev_data_values = prev_data[key]
                            prev_data_name = prev_data_values[1]
                            if prev_data_name == name:
                                if args.override:
                                    gender = data[0]
                                    binary_genders = get_gender(gender)
                                    writer.writerow(
                                        [*prev_data_values, *binary_genders])
                                else:
                                    writer.writerow(
                                        [*prev_data_values, data[0], data[1], data[2]])
                                break
            print("Done!\n")


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Bulk genderize.io script')
  required = parser.add_argument_group('required arguments')

  required.add_argument('-i','--input', help='Input file name', required=True)
  required.add_argument('-o','--output', help='Output file name', required=True)
  parser.add_argument('-k','--key', help='API key', required=False, default="NO_API")
  parser.add_argument('-c','--catch', help='Try to handle errors gracefully', required=False, action='store_true', default=True)
  parser.add_argument('-a','--auto', help='Automatically complete gender for identical first_name', required=False, action='store_true', default=False)
  parser.add_argument('-nh','--noheader', help='Input has no header row', required=False, action='store_true', default=False)
  parser.add_argument('-OVR', '--override', help='override default column output.', required=False, action='store_true', default=False)

  genderize(parser.parse_args())
