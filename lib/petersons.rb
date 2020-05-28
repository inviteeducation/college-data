require 'net/http'
require 'fileutils'
require 'zip'
require 'csv'

class Petersons

  def self.month_day(month_column, day_column)
    lambda do |row|
      month = row[month_column.to_s]
      day = row[day_column.to_s]
      if month and day
        [month, day].map { |part| part.rjust(2, '0') }.join('-')
      end
    end
  end

  def self.selectivity(column)
    lambda do |row|
      value = row[column.to_s]
      if value
        value = %w(NONC MIN MOD VERY MOST).index(value.upcase)
        value ? value.to_i + 1 : value # 1-based
      end
    end
  end

  def self.sat_within_range_or_nil(column)
    lambda do |row|
      value = row[column.to_s].to_i
      (200..800).include?(value) ? value : nil
    end
  end

  def self.act_within_range_or_nil(column)
    lambda do |row|
      value = row[column.to_s].to_i
      (1..36).include?(value) ? value : nil
    end
  end

  def self.if_year(year, translation)
    lambda do |row|
      @@records[row[ID_FIELD]][:petersons_last_year_surveyed].to_i >= year ? translation.call(row) : nil
    end
  end

  def self.standard_translation(column)
    lambda { |row| row[column.to_s] }
  end

  def self.flag(column, flag_value='X')
    lambda { |row| row[column.to_s] == flag_value }
  end

  def self.add(*columns)
    lambda do |row|
      columns.inject(0) { |sum, col| sum + Integer(row[col.to_s]) } rescue nil
    end
  end

  DIR = "#{File.expand_path File.dirname(__FILE__)}/../data/"
  FILES = [
      'UX_INST',
      'UG_ADMIS',
      'UG_ENTR_EXAMS',
      'UG_ENTR_EXAM_ASGNS',
      'UG_ENROLL',
      'UG_FACULTY',
      'UG_FIN_AID'
  ]
  ID_FIELD = 'INUN_ID'
  MAPPINGS = {
    UX_INST: {
      ipeds_id: 'MAIN_IPEDS_CODE',
      petersons_id: ID_FIELD,
      petersons_last_year_surveyed: 'YEAR_LAST_SURVEYED'
    },
    UG_ADMIS: {
      applied: if_year(2014, standard_translation('AP_RECD_1ST_N')),
      accepted: if_year(2014, standard_translation('AP_ADMT_1ST_N')),

      continuous_application_deadline: flag(:AP_DL_FRSH_I, 'C'),  
      application_deadline: month_day(:AP_DL_FRSH_MON, :AP_DL_FRSH_DAY),
      application_notification: month_day(:AP_NOTF_DL_FRSH_MON, :AP_NOTF_DL_FRSH_DAY),

      application_out_of_state_deadline: month_day(:AP_DL_NRES_MON, :AP_DL_NRES_DAY),
      application_out_of_state_notification: month_day(:AP_NOTF_DL_NRES_MON, :AP_NOTF_DL_NRES_DAY),

      application_early_decision_deadline: month_day(:AP_DL_EDEC_1_MON, :AP_DL_EDEC_1_DAY),
      application_early_decision_notification: month_day(:AP_NOTF_DL_EDEC_1_MON, :AP_NOTF_DL_EDEC_1_DAY),

      application_early_decision_other_deadline: month_day(:AP_DL_EDEC_2_MON, :AP_DL_EDEC_2_DAY),
      application_early_decision_other_notification: month_day(:AP_NOTF_DL_EDEC_2_MON, :AP_NOTF_DL_EDEC_2_DAY),

      application_early_action_deadline: month_day(:AP_DL_EACT_MON, :AP_DL_EACT_DAY),
      application_early_action_notification: month_day(:AP_NOTF_DL_EACT_MON, :AP_NOTF_DL_EACT_DAY),

      application_sat_act_deadline: month_day(:AP_SAT1_ACT_DL_MON, :AP_SAT1_ACT_DL_DAY),
      application_sat_subject_deadline: month_day(:AP_SAT2_DL_MON, :AP_SAT2_DL_DAY),

      selectivity: selectivity(:AD_DIFF_ALL)
    },
    UG_ENTR_EXAMS: {
      sat_reading_25: if_year(2014, sat_within_range_or_nil(:SAT1_VERB_25TH_P)),
      sat_reading_75: if_year(2014, sat_within_range_or_nil(:SAT1_VERB_75TH_P)),
      sat_reading_mean: if_year(2014, sat_within_range_or_nil(:SAT1_VERB_MEAN)),

      sat_math_25: if_year(2014, sat_within_range_or_nil(:SAT1_MATH_25TH_P)),
      sat_math_75: if_year(2014, sat_within_range_or_nil(:SAT1_MATH_75TH_P)),
      sat_math_mean: if_year(2014, sat_within_range_or_nil(:SAT1_MATH_MEAN)),

      act_composite_25: if_year(2014, act_within_range_or_nil(:ACT_COMP_25TH_P)),
      act_composite_75: if_year(2014, act_within_range_or_nil(:ACT_COMP_75TH_P)),
      act_composite_mean: if_year(2014, act_within_range_or_nil(:ACT_MEAN)),

      sat_essay_policy: 'SAT_ESSAY'
    },
    UG_ENTR_EXAM_ASGNS: {
      entrance_exams_not_used_for_admissions: flag('ADMS_NOT_USED')
    },
    UG_ENROLL: {
      gpa_mean: ->(row) do
        # there are some GPAs in the data without decimal points
        value = row['FRSH_GPA'];
        if value.is_a?(String) and (value = value.strip).length > 1 and not value.include?('.')
          value = value[0] + '.' + value[1..-1]
        end
        value = value.to_f
        value > 0 && value <= 5.0 ? value : nil
      end,
      gpa_weighted: flag('FRSH_GPA_WEIGHTED'),
      students_undergraduate_male: if_year(2014, add('EN_UG_FT_MEN_N', 'EN_UG_PT_MEN_N')),
      students_undergraduate_female: if_year(2014, add('EN_UG_FT_WMN_N', 'EN_UG_PT_WMN_N')),
      students_undergraduate: if_year(2014, standard_translation('EN_TOT_UG_N')),
      retention_rate: if_year(2014, standard_translation('RETENTION_FRSH_P')),
      graduation_rate_bachelors_cohort: if_year(2014, standard_translation('GRS_BACH_ADJUST_N')),
      graduation_rate_bachelors_completers_150_pct: if_year(2014, standard_translation('GRS_BACH_TOT_N')),
      graduation_rate_certificate_cohort: if_year(2014, standard_translation('GRS_ASSOC_ADJUST_N')),
      graduation_rate_certificate_completers_150_pct: if_year(2014, standard_translation('GRS_2YR_LESS_150_N'))
    },
    UG_FACULTY: {
      student_faculty_ratio: if_year(2015, standard_translation('UG_RATIO'))
    },
    UG_FIN_AID: {
      undergraduate_inst_debt_average: 'UG_CLASS_AVG_DEBT_INST_D',
      undergraduate_inst_debt_percentage: 'UG_CLASS_LOAN_INST_P',
      undergraduate_state_debt_average: 'UG_CLASS_AVG_DEBT_STATE_D',
      undergraduate_state_debt_percentage: 'UG_CLASS_LOAN_STATE_P',
      undergraduate_private_debt_average: 'UG_CLASS_AVG_DEBT_PRIVATE_D',
      undergraduate_private_debt_percentage: 'UG_CLASS_LOAN_PRIVATE_P',
      domestic_profile_required: flag('FORM_DOM_CSS'),
      international_profile_required: flag('FORM_INTL_CSS')
    }
  }

  def initialize
    @@records = {}
  end

  def import

    FILES.each do |file|
      csv_file = File.join(DIR, "petersons_colleges/#{file}.txt")
      puts "Importing #{csv_file}"
      CSV.foreach(csv_file, col_sep: "\t", headers: true, encoding: 'iso-8859-1:UTF-8') do |row|
        import_row(row, file.to_sym)
      end
    end

    @@records.values.each do |attrs|
      # For now, we'll accept Peterson's values over IPEDS, but only if not null
      attrs.delete_if { |k,v| v.nil? }
      # Need both of the values or remove them
      unless complete?(attrs, :students_undergraduate_female, :students_undergraduate_male)
        if attrs[:students_undergraduate].present?
          attrs[:students_undergraduate_female] = attrs[:students_undergraduate].to_i - attrs[:students_undergraduate_male].to_i
          attrs[:students_undergraduate_male] = attrs[:students_undergraduate].to_i - attrs[:students_undergraduate_female].to_i
        else
          attrs = attrs.except(:students_undergraduate_female, :students_undergraduate_male)
        end
      end
      # Need both of the values or remove them
      unless complete?(attrs, :graduation_rate_bachelors_cohort, :graduation_rate_bachelors_completers_150_pct)
        attrs = attrs.except(:graduation_rate_bachelors_cohort, :graduation_rate_bachelors_completers_150_pct)
      end
      # Need both of the values or remove them
      unless complete?(attrs, :graduation_rate_certificate_cohort, :graduation_rate_certificate_completers_150_pct)
        attrs = attrs.except(:graduation_rate_certificate_cohort, :graduation_rate_certificate_completers_150_pct)
      end
      # Need both of the values or remove them
      unless complete?(attrs, :applied, :accepted)
        attrs = attrs.except(:applied, :accepted)
      end
      import_attributes(attrs)
    end

    puts 'Petersons college import complete'
  end

  def complete?(hash, *keys)
    defined_keys = keys.inject(0) { |sum, key| sum + (hash[key].present? ? 1 : 0) }
    defined_keys == 0 || defined_keys == keys.length
  end

  def normalize_value(value)
    # strip leading and trailing whitespace and substitute with nil if empty
    if value.is_a?(String)
      value.strip!
      value = nil if value.empty?
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
    # ensure :petersons_id is present
    mappings[:petersons_id] = ID_FIELD
    mappings
  end

  def import_row(row, mapping)
    attrs = to_attrs(row, mappings(mapping))
    id = attrs[:petersons_id]
    record = @@records[id] = @@records[id] || {}
    record.merge!(attrs)
  end

  def import_attributes(attrs, create_if_missing=false)
    college = create_if_missing ?
      College.find_or_initialize_by(ipeds_id: attrs[:ipeds_id]) :
      College.find_by(ipeds_id: attrs[:ipeds_id])
    if college
      college.update_attributes(attrs)
    else
      # puts "Skipping row as record not found for ipeds_id #{attrs[:ipeds_id]}"
    end
  end

end
