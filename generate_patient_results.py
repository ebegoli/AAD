#!/usr/bin/env python
#
""" Program will generate set of ICD-9 for certain age and gender, some vital signs and textual description of the diagnosis.
It has number of methods that randomly insert data quality problems such as:
* transposed height and weight
* atypical height for the sex
* impossible ICD 9 for sex (transposed sex)
* sub-par diagnosis description
* diagnosis not matching the ICD 9
* transposed systolic and diastolic blood pressure
Produced record consists of:
   dob, sex, weight, height, temperature, icd-9, diagnosis, systolic, diastolic  
"""

from datetime import datetime, date
import random
import csv
import time #for profiling


sex = ("m","f")
avg_weight = {"m":88.3,"f":74.7}
avg_height = {"m":177.0,"f":163.5}
avg_temperature = 98.6
avg_systolic = 120.0
avg_distolic = 80.0
ICD_9 = [   ("0024","Intravascular imaging of coronary vessels"),
            ("2509","Other diagnostic procedures on tongue"),
            ("260","Incision of salivary gland or duct"),
            ("3766","Insertion of implantable heart assist system"),
            ("4449","Other control of hemorrhage of stomach or duodenum"),
            ("7788","Other partial ostectomy, tarsals and metatarsals" ),
            ("7967","Debridement of open fracture site, tarsals and metatarsals"),
            ("7946","Closed reduction of separated epiphysis, tibia and fibula" ),
            ("8004","Arthrotomy for removal of prosthesis without replacement, hand and finger"),
            ("8138","Refusion of lumbar and lumbosacral spine, anterior column, posterior technique"),
            ("8258","Other hand muscle transfer or transplantation"),
            ("0721","Excision of lesion of adrenal gland"),
            ("8202","Myotomy of hand"),
            ("8850","Angiocardiography, not otherwise specified"),
            ("9205","Cardiovascular and hematopoietic scan and radioisotope function study"),
            ("4104","Autologous hematopoietic stem cell transplant without purging"),
            ("4050","Radical excision of lymph nodes, not otherwise specified"),
            ("7864","Removal of implanted devices from bone, carpals and metacarpals"),
            ("3594","Creation of conduit between atrium and pulmonary artery"),
            ("3451","Decortication of lung"),
            ("3423","Biopsy of chest wall"),
            ]

m_ICD_9 = ( ("634","Epididymectomy"),("6018","Other diagnostic procedures on prostate and periprostatic tissue") )

f_ICD_9 = (("7021","Vaginoscopy"),("6592","Transplantation of ovary"), ("6942","Closure of fistula of uterus"))

#NOTE: 6592,6942, 7021, m: 6018, 634

def get_weight(avg_weight,lower_percent=0.75,higher_percent=1.25):
    """ Gets a randomly choosen weight within a certain range based on the national averages."""
    return round(random.uniform(lower_percent*avg_weight,higher_percent*avg_weight),1)

def get_height(avg_height,lower_percent=0.9,higher_percent=1.1):
    """Gets a randomly choosen height within a certain range based on the national averages."""
    return round(random.uniform(lower_percent*avg_height,higher_percent*avg_height),1)

def get_year(oldest=90, youngest=21):
    """Gets a randomly choosen age. Change defaults to change selection range. Assumed adults."""
    current_year = date.today().year + 1
    return date(year=random.randint(current_year-oldest,current_year-youngest))

def get_systolic(low_threshold=0.95,high_threshold=1.1):
    """Gets a randomly choosen systolic blood pressure. Change defaults to change selection range."""
    return random.randint(avg_systolic*low_threshold,avg_systolic*high_threshold)

def get_distolic(low_threshold=0.95,high_threshold=1.1):
    """Gets a randomly choosen distolic blood pressure. Change defaults to change selection range."""
    return random.randint(avg_distolic*low_threshold,avg_distolic*high_threshold)

def get_temperature(low_threshold=0.98,high_threshold=1.05):
    """Gets a randomly choosen temperature. Change defaults to change selection range."""
    return round(random.uniform(avg_temperature*low_threshold,avg_distolic*high_threshold),1)

def get_icd(sex="f"):
    """Gets a randomly selected ICD_9. TODO: do a sex appropriate ICD. (i.e., no prostate exam possible for female."""
    return random.choice(ICD_9)

def get_dob(start_year=1945,to_year=1995):
    """Generates a date between a specified range (default is [1945,1995])."""
    start_date = date(start_year,1,1)
    end_date = date(to_year,12,31)
    ordinal_date = random.randint(start_date.toordinal(),end_date.toordinal())
    return date.fromordinal(ordinal_date)

def default_constructor(sex_denom,avg_w, avg_h, icd):
    """ Default method for construction of the 'good' record."""
    return (map(str, (get_dob(),sex_denom,get_weight(avg_w),get_height(avg_h),get_temperature(),icd[0],
        icd[1],get_systolic(), get_distolic())))


def get_entry(entry_constructor=default_constructor):
    """ Creates a standard row. It accepts constructor method to produce row. This is where we can pass different methods to\
            to produce other entries with various problems. """
    sex_denom = random.choice(sex)
    avg_w = avg_weight[sex_denom]
    avg_h = avg_height[sex_denom]
    icd = get_icd()
    return entry_constructor(sex_denom,avg_w,avg_h,icd)

def get_flipped_bp(sex_denom,avg_w, avg_h, icd):
    """ data quality scenario 1: flipped systolic and diastolic blood pressure """
    return (map(str, (get_dob(),sex_denom,get_weight(avg_w),get_height(avg_h),get_temperature(),icd[0],
            icd[1],get_distolic(),get_systolic())))


def get_mismatched_icd9_desc(sex_denom,avg_w, avg_h, icd):
    """ data quality scenario 2: diagnosis description not matching ICD-9 """
    icd1 = icd
    temp_list = list(ICD_9)
    temp_list.remove(icd1)
    icd2 = random.choice(temp_list)
    return (map(str, (get_dob(),sex_denom,get_weight(avg_w),get_height(avg_h),get_temperature(),icd1[0],
            icd2[1],get_systolic(),get_distolic())))

def get_mismatched_height(sex_denom,avg_w, avg_h, icd):
    """ data quality scenario 3: height flipped for sex """
    other_sex = "m"
    if sex == "m":
       other_sex = "f"
    avg_w = avg_weight[other_sex]
    avg_h = avg_height[other_sex]
    return (map(str, (get_dob(),sex_denom,get_weight(avg_w),get_height(avg_h),get_temperature(),icd[0],
            icd[1],get_systolic(),get_distolic())))

def get_flipped_height_weight(sex_denom,avg_w, avg_h, icd):
    """ data quality scenario 4: height and weight entries are flipped on the record """
    return (map(str, (get_dob(),sex_denom,get_height(avg_h),get_weight(avg_w),get_temperature(),icd[0],
            icd[1],get_systolic(),get_distolic())))

def get_low_quality_desc(sex_denom,avg_w, avg_h, icd):
    """ data quality scenario 5: diagnosis description is lacking. """
    desc = icd[1]
    tokens = desc.split(" ")
    print desc
    print tokens
    new_desc = desc
    #make a loss of text as certain percent of loss of tokens
    loss_token_count = int(len(tokens)*0.25)
    replace_tokens = random.sample(tokens,loss_token_count)
    print "loss count ", loss_token_count, " losing tokens ", replace_tokens
    for token in replace_tokens:
        new_desc = new_desc.replace(token,"")  
    print "depleted ",new_desc
    random_token_to_misspell = random.choice(new_desc.split())
    random_location = random.randint(0,len(new_desc))
    random_character = random.choice(['z','k','r','y','g','w'])
    new_desc = new_desc[:random_location]+random_character+new_desc[random_location:]
    print "after misspelling ",new_desc
    return (map(str, (get_dob(),sex_denom,get_weight(avg_h),get_height(avg_w),get_temperature(),icd[0],
            new_desc,get_systolic(),get_distolic())))

def get_wrong_icd9_for_sex(sex_denom,avg_w, avg_h, icd):
    """ # data quality scenario 6: ICD-9 not meaningful for particular sex """
    if sex_denom == "f":
        icd = random.choice(m_ICD_9)
    else:
        icd = random.choice(f_ICD_9)
    return (map(str, (get_dob(),sex_denom,get_height(avg_h),get_weight(avg_w),get_temperature(),icd[0],
            icd[1],get_systolic(),get_distolic())))


if __name__ == "__main__":
    """ Excetutes the generation of records entries and file creation."""
    start_time = time.time()
    pdv_file = open("output.pdv","wb")
    pdv_writer = csv.writer(pdv_file, delimiter="|", lineterminator="\n")
    pdv_record_file = open("dq_record.pdv","wb")
    pdv_record_writer = csv.writer(pdv_record_file, delimiter="|", lineterminator="\n")
    header = ("dob","sex","weight","height","temp","icd","icd desc","distolic","systolic")
    record_header = ("index","problem")
    print header
    pdv_writer.writerow(header)
    pdv_record_writer.writerow(record_header)
    
    num_of_records = 100000
    population_index = list(range(num_of_records))
    random.shuffle(population_index)
    
    mismatched_height_density = int(num_of_records*0.001)
    mismatched_icd_density = int(num_of_records*0.0001)
    flipped_bp_density = int(num_of_records*0.002)
    flipped_hw_density = int(num_of_records*0.005)
    wronq_icd_density = int(num_of_records*0.001)
    poor_icd_desc_density = int(num_of_records*0.008)


    mismatched_height_freq = population_index[-mismatched_height_density:]
    population_index[-mismatched_height_density:] = []

    mismatched_icd_freq = population_index[-mismatched_icd_density:]
    population_index[-mismatched_icd_density:] = []

    flipped_bp_freq = population_index[-flipped_bp_density:]
    population_index[-flipped_bp_density:] = []

    flipped_hw_freq = population_index[-flipped_hw_density:]
    population_index[-flipped_hw_density:] = []

    wrong_icd_freq = population_index[-wronq_icd_density:]
    population_index[-wronq_icd_density:] = []

    poor_icd_desc_freq = population_index[-poor_icd_desc_density:]
    population_index[-poor_icd_desc_density:] = []


    print "for %d records setup: mimatched height freq %d, mismatched_icd freq %d, flipped bp %d, flipped hw %d, flipped icd %d" %\
          (num_of_records,len(mismatched_height_freq),len(mismatched_icd_freq), len(flipped_bp_freq),len(flipped_hw_freq),len(wrong_icd_freq))

    for i in xrange(0,num_of_records):
        entry = None
        dq_error_record = None
        #print "loop: ",i
        if i in mismatched_height_freq:
            dq_error_record = (i,"mismatched height for sex")
            entry = get_entry(entry_constructor=get_mismatched_height)
        if i in mismatched_icd_freq:
            dq_error_record = (i,"mismatched icd code and description")
            entry = get_entry(entry_constructor=get_mismatched_icd9_desc)
        elif i in flipped_bp_freq:
            dq_error_record = (i,"flipped bp")
            entry = get_entry(entry_constructor=get_flipped_bp)
        elif i in flipped_hw_freq:
            dq_error_record = (i,"flipped height and weight")
            entry = get_entry(entry_constructor=get_flipped_height_weight)
        elif i in wrong_icd_freq:
            dq_error_record = (i,"wrong icd for sex")
            entry = get_entry(entry_constructor=get_wrong_icd9_for_sex)
        elif i in poor_icd_desc_freq:
            dq_error_record = (i,"poor icd desc")
            entry = get_entry(entry_constructor=get_low_quality_desc)
        else:
            entry= get_entry(entry_constructor=default_constructor)

        #print entry
        if dq_error_record:
            #print dq_error_record
            pdv_record_writer.writerow(dq_error_record )

        pdv_writer.writerow(entry)
        #Flushing to clear up memory on very large files
        if i%1000 == 0:
            pdv_file.flush()
            pdv_record_file.flush()
    print "file created."
    print("---execution took: {:.2} seconds ---".format( (time.time() - start_time)) )
