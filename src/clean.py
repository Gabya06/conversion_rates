from load import *
from functions import *

def usage():
	print """
	To clean file: python clean.py [input_file output_file]
	ex: python load.py input_file.xlsx, output_file.csv
	"""
if __name__ == '__main__':
	if len(sys.argv) !=3:
		usage()
		sys.exit(2)
	try:
		file_name = sys.argv[1] # clean data file
		out_file = sys.argv[2] # csv file to write to

	except IOError:
		sys.stderr.write("Error: cant read input file %s.\n" % arg)
		sys.exit(1)


	# set directory & read data
	os.chdir("/Users/Gabi/Documents/dev/conv_rates/")
	data_path = os.getcwd()+'/data/input_data/'


	partner_file = 'Opportunities_with_Partners_06282016.csv'
	partner_ids_exclude = pd.read_csv(data_path + partner_file)
	partner_ids_exclude.dropna(inplace = True)
	p_cols = partner_ids_exclude.columns
	p_cols = [clean_column(c) for c in p_cols]
	partner_ids_exclude.columns = p_cols
	# exclude partner opportunities
	id_exclude = partner_ids_exclude.OpportunityID.drop_duplicates()

	filename = file_name
	print filename
	xl = pd.ExcelFile(filename)

	# check out sheets
	print "Found sheet names: ", xl.sheet_names
	sheet_name = xl.sheet_names[0]
	'''
	Read data file & partner ids
	'''
	data = xl.parse(sheet_name)

	df = clean_dat(dat = data, exclude = id_exclude)
	df.to_csv(out_file, encoding = 'utf-8')
