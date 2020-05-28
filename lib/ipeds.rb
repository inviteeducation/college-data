require 'net/http'
require 'fileutils'
require 'zip'
require 'csv'

class IPEDS

  def self.normalize_url(column)
    lambda do |row|
      url = row[column]
      url.strip!
      if url.empty?
        url = nil
      elsif !(url =~ /\Ahttps?:\/\/.+\z/i)
        url = 'http://' + url
      end
      url
    end
  end

  def self.above_zero_or_nil(column)
    lambda { |row| value = row[column].to_i; value > 0 ? value : nil }
  end

  def self.true_if_one(column)
    lambda { |row| row[column].to_i == 1 ? true : false }
  end

  def self.true_if_any_one(columns)
    lambda do |row|
      columns.map { |c| row[c].to_i }.any? { |i| i == 1 }
    end
  end

  CACHE_DIR = Rails.root.join('tmp', 'ipeds')
  # need to use a proxy to access the IPEDS database from a Amazon EC2 instances
  BASE_URL = 'http://nces.ed.gov/ipeds/datacenter/data/'
  FILES = [
    ['HD2018', :HD],
    ['IC2018', :IC],
    ['IC2018_AY', :IC_AY],
    ['ADM2018', :ADM],
    ['EFFY2018', :EFFY],
    ['EF2018D', :EF_D],
    ['GR2018', :GR],
    ['C2018_A', :C_A],
    ['SFA1718_P1', :SFA]
  ]
  MAPPINGS = {
    HD: {
      active: true_if_one('CYACTIVE'),
      name: 'INSTNM',
      address: 'ADDR',
      city: 'CITY',
      state: 'STABBR',
      zip: 'ZIP',
      latitude: 'LATITUDE',
      longitude: 'LONGITUD',
      nces_locale_code: above_zero_or_nil('LOCALE'),
      phone: 'GENTELE',
      url: normalize_url('WEBADDR'),
      admissions_url: normalize_url('ADMINURL'), # we currently don't use this
      financial_aid_url: normalize_url('FAIDURL'), # we currently don't use this
      application_url: normalize_url('APPLURL'), # we currently don't use this
      net_price_calculator_url: normalize_url('NPRICURL'), # we currently don't use this
      level: above_zero_or_nil('ICLEVEL'),
      control: above_zero_or_nil('CONTROL'),
      category: above_zero_or_nil('INSTCAT') # we currently don't use this
    },
    IC: {
      religious_affiliation: above_zero_or_nil('RELAFFIL'), # we currently don't use this
      offers_associates_degree: true_if_one('LEVEL3'),
      offers_bachelors_degree: true_if_one('LEVEL5'),
      offers_masters_degree: true_if_one('LEVEL7'),
      offers_doctors_degree: true_if_any_one(%w(LEVEL17 LEVEL18 LEVEL19)),
      offers_undergraduate_certificate: true_if_any_one(%w(LEVEL1 LEVEL2 LEVEL4)),
      offers_graduate_certificate: true_if_any_one(%w(LEVEL6 LEVEL8)),
    },
    IC_AY: {
      tuition_in_district: 'TUITION1',
      tuition_in_state: 'TUITION2',
      tuition_out_of_state: 'TUITION3',
      fees_in_district: 'FEE1',
      fees_in_state: 'FEE2',
      fees_out_of_state: 'FEE3',
      books_and_supplies: 'CHG4AY3',
      room_and_board_on_campus: 'CHG5AY3',
      room_and_board_off_campus_without_family: 'CHG7AY3',
      other_expenses_on_campus: 'CHG6AY3',
      other_expenses_off_campus_without_family: 'CHG8AY3',
      other_expenses_off_campus_with_family: 'CHG9AY3'
    },
    ADM: {
      applied: 'APPLCN',
      accepted: 'ADMSSN',
      enrolled: 'ENRLT',
      sat_reading_25: 'SATVR25',
      sat_reading_75: 'SATVR75',
      sat_math_25: 'SATMT25',
      sat_math_75: 'SATMT75',
      act_composite_25: 'ACTCM25',
      act_composite_75: 'ACTCM75',
      act_english_25: 'ACTEN25', # we currently don't use this
      act_english_75: 'ACTEN75', # we currently don't use this
      act_math_25: 'ACTMT25', # we currently don't use this
      act_math_75: 'ACTMT75' # we currently don't use this
    },
    EFFY: {
      students_undergraduate: 'EFYTOTLT',
      students_undergraduate_male: 'EFYTOTLM',
      students_undergraduate_female: 'EFYTOTLW',
    },
    EF_D: {
      student_faculty_ratio: 'STUFACR',
      retention_rate: 'RET_PCF'
    },
    SFA_COUNT_AND_AVERAGE: {
      grant_or_scholarship_undergraduate: 'UAGRNT', # we currently don't use this
      grant_federal_pell_undergraduate: 'UPGRNT', # we currently don't use this
      student_loan_federal_undergraduate: 'UFLOAN', # we currently don't use this

      grant_or_scholarship: 'AGRNT_',
      grant_federal: 'FGRNT_', # we currently don't use this
      grant_federal_pell: 'PGRNT_', # we currently don't use this
      grant_federal_other: 'OFGRT_', # we currently don't use this
      grant_or_scholarship_state_local: 'SGRNT_', # we currently don't use this
      grant_or_scholarship_institutional: 'IGRNT_', # we currently don't use this

      student_loan: 'LOAN_', # we currently don't use this
      student_loan_federal: 'FLOAN_', # we currently don't use this
      student_loan_other: 'OLOAN_' # we currently don't use this
    }
  }

  def count_and_average(row, count_column, total_column, average_column, attrs, count_field, average_field)
    ipeds_id = row['UNITID']
    count, total, average = row[count_column], row[total_column], row[average_column]
    unless count.blank?
      count, average = count.to_i, nil
      unless total.blank?
        total = total.to_i
        calculated_average = count > 0 ? (total * 1.0 / count).round : 0.0
        if average.blank?
          # puts "No #{average_column} for ipeds_id #{ipeds_id}, so calculating from #{count_column} and #{total_column}"
          average = calculated_average
        else
          average = average.to_i
          puts "Warning: calculated average #{calculated_average} != #{average} for IPEDS ID #{ipeds_id}" unless average == calculated_average
        end
      else
        # puts "No #{total_column} for ipeds_id #{ipeds_id}"
      end
      attrs[count_field.to_sym] = count
      attrs[average_field.to_sym] = average
    else
      attrs[count_field.to_sym] = attrs[average_field.to_sym] = nil
      # puts "No #{count_column} for ipeds_id #{ipeds_id}"
    end
  end

  def handle_sfa_row(row)
    ipeds_id = row['UNITID']
    attrs = { ipeds_id: ipeds_id }

    mappings = {
      aid_cohort: 'SCUGFFN', # we currently don't use this
      aid_cohort_undergraduate: 'SCUGRAD', # we currently don't use this
      aid_count: 'ANYAIDN' # we currently don't use this
    }
    mappings.each do |field, column|
      value = row[column].to_i
      attrs[field] = value > 0 ? value : nil
    end

    col_types_1 = %w(N T A)
    col_types_2 = %w(G T A)
    field_suffixes = %w(count amount)

    # populate counts and amounts for various aid fields
    self.class::MAPPINGS[:SFA_COUNT_AND_AVERAGE].each do |field_prefix, column_prefix|
      cols =  col_types_1.map { |type| "#{column_prefix}#{type}" }
      fields = field_suffixes.map { |suffix| "#{field_prefix}_#{suffix}" }
      count_and_average(row, *cols, attrs, *fields)
    end

    # sniff public or private institution
    public_school = row['GISTN2'].blank? ? false : true

    cols =  col_types_1.map { |type| public_school ? "GIST#{type}2" : "GRNT#{type}2" }
    fields = field_suffixes.map { |suffix| "grant_or_scholarship_segment_#{suffix}" }
    count_and_average(row, *cols, attrs, *fields)

    cols =  col_types_2.map { |type| public_school ? "GIS4#{type}2" : "GRN4#{type}2" }
    fields = field_suffixes.map { |suffix| "grant_or_scholarship_segment_federal_#{suffix}" }
    count_and_average(row, *cols, attrs, *fields)

    (1..5).each do |level|
      cols =  col_types_2.map { |type| public_school ? "GIS4#{type}#{level}2" : "GRN4#{type}#{level}2" }
      fields = field_suffixes.map { |suffix| "grant_or_scholarship_segment_federal_income_#{level}_#{suffix}" }
      count_and_average(row, *cols, attrs, *fields)

      col = public_school ? "NPIS4#{level}2" : "NPT4#{level}2"
      value = ((value = row[col].to_i) > 0) ? value : nil
      # if private school, value applies to both in state and out of state, else only in state
      unless public_school
        attrs["net_price_out_of_state_income_#{level}".to_sym] = value
      end
      attrs["net_price_in_state_income_#{level}".to_sym] = value
    end

    col = public_school ? 'NPIST2' : 'NPGRN2'
    value = ((value = row[col].to_i) > 0) ? value : nil
    # if private school, value applies to both in state and out of state, else only in state
    unless public_school
      attrs[:net_price_out_of_state] = value
    end
    attrs[:net_price_in_state] = value

    import_attributes(attrs)
  end

  def selected_files(only = nil)
    if only.blank?
      files = FILES
    else
      only.map! { |o| o.to_s.upcase.to_sym }
      files = FILES.select do |file_config|
        file = file_config[1]
        only.include?(file)
      end
    end
    files
  end

  def download(only = nil)
    # ensure cache directory exists
    FileUtils.mkdir_p(CACHE_DIR)

    # download, cache and unzip files
    puts "Downloading and unzipping IPEDS files to #{CACHE_DIR}"
    selected_files(only).each do |file_config|
      filename = file_config[0] + '.zip'
      cached_file = File.join(CACHE_DIR, filename)
      uri = URI(BASE_URL + filename)
      resp = Net::HTTP.get_response(uri)
      # follow redirect if necessary
      resp = Net::HTTP.get_response(URI(resp.header['location'])) if resp.code == "302" || resp.code == "301"
      if resp.kind_of? Net::HTTPSuccess
        # cache
        File.open(cached_file, 'wb') do |file|
          file.write(resp.body)
          puts "Downloaded #{filename}"
        end
      else
        puts "Unable to download #{filename}"
        # exit if unable to download all files
        abort "Aborting import as not all files were downloaded"
      end
      # unzip
      Zip::File.open(cached_file) do |zip|
        # use the revised csv file if it exists
        entry = zip.glob('*_rv.csv').first || zip.glob('*.csv').first
        if entry
          unzipped_file = File.join(CACHE_DIR, entry.name.gsub('_rv',''))
          File.delete(unzipped_file) if File.exists?(unzipped_file)
          zip.extract(entry, unzipped_file)
          repair_file(entry, unzipped_file)
          puts "Unzipped #{entry.name}"
        end
      end
    end
  end

  def import(only = nil)
    download(only)
    programs_reset = false
    selected_files(only).each do |file_config|
      filename = file_config[0]
      mapping = file_config[1]
      csv_file = File.join(CACHE_DIR, filename.downcase + '.csv')
      puts "Importing #{csv_file}"
      CSV.foreach(csv_file, headers: true, header_converters: ->(h) { h.strip }, encoding: 'iso-8859-1:UTF-8') do |row|
        if mapping == :HD
          import_row(row, mapping, true)
        elsif mapping == :EFFY
          handle_effy_row(row)
        elsif mapping == :EF_D
          import_row(row, mapping)
        elsif mapping == :GR
          handle_gr_row(row)
        elsif mapping == :C_A
          # only reset the programs if we're about to start re-importing them
          unless programs_reset
            puts 'Resetting programs before re-importing'
            Program.delete_all
            programs_reset = true
          end
          handle_c_a_row(row)
        elsif mapping == :SFA
          handle_sfa_row(row)
        else
          import_row(row, mapping)
        end
      end
    end
  end

  def normalize_value(value)
    # strip leading and trailing whitespace and substitute with nil if empty
    if value.is_a?(String)
      value.strip!
      # empty values seem to occasionally be represented by a period (e.g. test scores)
      value = nil if value.empty? or value == '.'
    end
    value
  end

  def to_attrs(row, mappings)
    row = row.to_hash
    attrs = {}
    mappings.each do |to, from|
      if from.respond_to?(:call)
        value = from.call(row)
      else
        value = row[from.to_s]
      end
      attrs[to] = normalize_value(value)
    end
    attrs
  end

  def mappings(mapping)
    mappings = MAPPINGS[mapping]
    # ensure :ipeds_id is present
    mappings[:ipeds_id] = 'UNITID'
    mappings
  end

  def import_row(row, mapping, create_if_missing=false)
    attrs = to_attrs(row, mappings(mapping))
    import_attributes(attrs, create_if_missing)
  end

  def import_attributes(attrs, create_if_missing=false)
    college = create_if_missing ?
      College.find_or_initialize_by(ipeds_id: attrs[:ipeds_id]) :
      College.find_by(ipeds_id: attrs[:ipeds_id])
    if college
      college.update_attributes(attrs)
    else
      #puts "Skipping row as record not found for ipeds_id #{attrs[:ipeds_id]}"
    end
  end

  def handle_effy_row(row)
    level = row['EFFYLEV'].to_i
    # represents undergraduate total
    import_row(row, :EFFY) if level == 2
  end

  def handle_gr_row(row)
    type = row['GRTYPE'].to_i
    case type
    when 2
      field = :graduation_rate_bachelors_cohort
    when 3
      field = :graduation_rate_bachelors_completers_150_pct
    when 29
      field = :graduation_rate_certificate_cohort
    when 30
      field = :graduation_rate_certificate_completers_150_pct
    else
      return # skip row
    end
    attrs = { ipeds_id: row['UNITID'] }
    attrs[field] = normalize_value(row['GRTOTLT'])
    import_attributes(attrs)
  end

  def handle_c_a_row(row)
    ipeds_id = row['UNITID']
    college = College.find_by(ipeds_id: ipeds_id)
    if college
      cip_code = row['CIPCODE'].strip
      level = row['AWLEVEL']
      major_num = row['MAJORNUM'].to_i
      program = Program.find_or_initialize_by(college_id: college.id, cip_code: cip_code, level: level)
      if major_num == 1
        program.awards = row['CTOTALT']
      end
      program.save
    else
      #puts "Skipping row as record not found for ipeds_id #{ipeds_id}"
    end
  end

  # convenience method for exploring files in development
  def load_files(files, only_ids=nil)
    only_ids = only_ids.map { |id| id.to_s } if only_ids
    loaded = {}
    FILES.each do |file_config|
      filename = file_config[0]
      mapping = file_config[1]
      if files.include?(mapping)
        loaded[mapping] = []
        csv_file = File.join(CACHE_DIR, filename.downcase + '.csv')
        puts "Loading #{csv_file}"
        CSV.foreach(csv_file, headers: true, encoding: 'iso-8859-1:UTF-8') do |row|
          if only_ids.nil? or only_ids.include?(row[0])
            loaded[mapping] << row.to_hash
          end
        end
      end
    end
    loaded
  end

  def repair_file(entry, path)
    # fix a CSV::MalformedCSVError: Missing or stray quote in line with the hd2016.csv provisional file
    if entry.name == "hd2016.csv"
      # read file and fix stray quote, then write contents to the file
      contents = File.read(path, encoding: 'iso-8859-1:UTF-8')
      contents.gsub!('""Acting" Director"', '"Acting Director"')

      File.open(path, 'w:iso-8859-1:UTF-8') { |f| f.write(contents) }
    end
  end

end
