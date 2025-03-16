import pandas as pd
from datetime import datetime, timedelta
import random
import os
import json

class Config:
    STROKES = {
        'Butterfly': [50, 100, 200],
        'Freestyle': [50, 100, 200, 400, 800, 1500],
        'Backstroke': [50, 100, 200],
        'Breaststroke': [50, 100, 200],
        'Individual Medley': [200, 400]
    }
    
    COMPETITIONS = {
        'National': {'meets': 12, 'pool': '50m', 'entries': (40, 60)},
        'Province': {'meets': 24, 'pool': '25m', 'entries': (30, 45)},
        'Local': {'meets': 52, 'pool': '25m', 'entries': (20, 35)}
    }
    
    REGIONS = {
        'USA': ['NYC', 'LA', 'CHI'],
        'AUS': ['SYD', 'MEL', 'BRI'],
        'GBR': ['LON', 'MAN', 'BIR'],
        'CHN': ['BEJ', 'SHA', 'GUA'],
        'JPN': ['TOK', 'OSA', 'KYO'],
        'CAN': ['TOR', 'VAN', 'MTL']
    }
    
    EDUCATION_LEVELS = ['High School', 'Bachelors', 'MBA']
    
    SKILL_LEVELS = {
        'elite': (0.98, 1.05),
        'competitive': (1.05, 1.15),
        'regular': (1.15, 1.3)
    }
    
    VACCINATION = ['Fully Vaccinated', 'Partially Vaccinated', 'Not Vaccinated', 'Exemption']
    SPONSORS = ['Speedo', 'Arena', 'TYR Sport', 'Nike Swimming', 'None']
    AUTHORITIES = ['FINA', 'National Federation', 'Regional Authority']
    VIOLATIONS = {'None': False, 'Minor Violation': False, 'Major Violation': True, 'Multiple Violations': True}

class Education:
    def __init__(self, age):
        if age < 18:
            self.level = 'High School'
        elif age < 25:
            self.level = 'Bachelors'
        else:
            # 30% chance of MBA for those 25 and older
            self.level = 'MBA' if random.random() < 0.3 else 'Bachelors'

class Swimmer:
    def __init__(self, is_core=False):
        self.id = f'SWM{random.randint(1000, 9999)}'
        self.name = f"{'Elite_' if is_core else ''}Swimmer_{self.id}"
        self.birth_date = datetime.now() - timedelta(days=random.randint(12*365, 35*365))
        self.age = (datetime.now() - self.birth_date).days // 365
        self.gender = random.choice(['Men', 'Women'])
        self.skill_level = 'elite' if is_core else random.choice(['competitive', 'regular'])
        
        self._set_location()
        self.education = Education(self.age)
        self._set_background(is_core)
        self._init_performance()
    
    def _set_location(self):
        self.birth_country = random.choice(list(Config.REGIONS.keys()))
        if random.random() < 0.2:  # Foreign participation
            self.citizenship = random.choice([c for c in Config.REGIONS.keys() if c != self.birth_country])
            self.residency_years = random.randint(2, 10)
            self.approving_authority = random.choice(Config.AUTHORITIES)
        else:
            self.citizenship = self.birth_country
            self.residency_years = self.age
            self.approving_authority = None
        
        self.region = random.choice(Config.REGIONS[self.citizenship])
        self.club = f'Club_{self.region}'
    
    def _set_background(self, is_core):
        self.criminal_record = random.choice(list(Config.VIOLATIONS.keys()))
        self.is_disqualified = Config.VIOLATIONS[self.criminal_record]
        self.vaccination_status = 'Fully Vaccinated' if is_core else random.choice(Config.VACCINATION)
        self.sponsor = random.choice(Config.SPONSORS[:-1]) if is_core else random.choice(Config.SPONSORS)
    
    def _init_performance(self):
        self.specialties = random.sample(list(Config.STROKES.keys()), random.randint(2, 3))
        self.rankings = {stroke: {f'rank{i}': 0 for i in range(1, 6)} for stroke in Config.STROKES}
        self.personal_bests = {stroke: {} for stroke in Config.STROKES}
        self.historical_times = {stroke: {d: [] for d in distances} for stroke, distances in Config.STROKES.items()}
        self.valid_records = {stroke: {d: True for d in distances} for stroke, distances in Config.STROKES.items()}

class Competition:
    @staticmethod
    def format_time(seconds):
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}:{remaining_seconds:06.2f}" if minutes > 0 else f"{remaining_seconds:.2f}"
    
    @staticmethod
    def generate_schedule(start_date, end_date):
        competitions = []
        for level, info in Config.COMPETITIONS.items():
            days_between = 365 // info['meets']
            for i in range(info['meets']):
                meet_date = start_date + timedelta(days=i * days_between + random.randint(-3, 3))
                if start_date <= meet_date <= end_date:
                    country = random.choice(list(Config.REGIONS.keys()))
                    competitions.append({
                        'date': meet_date,
                        'level': level,
                        'competition_id': f'{level[:3].upper()}_{i+1:03d}',
                        'country': country,
                        'region': random.choice(Config.REGIONS[country]),
                        'pool_type': info['pool']
                    })
        return sorted(competitions, key=lambda x: x['date'])
    
    @staticmethod
    def generate_results(comp_info, stroke, distance, swimmers, num_entries):
        base_time = 50.0 * (distance/100)
        eligible_swimmers = [s for s in swimmers[:num_entries] 
                           if not s.is_disqualified 
                           and (s.residency_years >= 2 if s.citizenship != s.birth_country else True)
                           and (s.vaccination_status == 'Fully Vaccinated' if comp_info['level'] == 'National' else True)]
        
        results = []
        for swimmer in eligible_swimmers:
            if stroke in swimmer.specialties or random.random() < 0.2:
                time = base_time * random.uniform(*Config.SKILL_LEVELS[swimmer.skill_level])
                Competition._update_records(swimmer, stroke, distance, time)
                results.append(Competition._create_entry(swimmer, stroke, distance, time))
        
        results.sort(key=lambda x: float(x['Time'].replace(':', '')))
        Competition._update_rankings(results, swimmers)
        return results
    
    @staticmethod
    def _update_records(swimmer, stroke, distance, time):
        swimmer.historical_times[stroke][distance].append(time)
        if len(swimmer.historical_times[stroke][distance]) > 50:
            swimmer.historical_times[stroke][distance] = swimmer.historical_times[stroke][distance][-50:]
        
        if distance not in swimmer.personal_bests[stroke] or time < swimmer.personal_bests[stroke][distance]:
            swimmer.personal_bests[stroke][distance] = time
            swimmer.valid_records[stroke][distance] = True
    
    @staticmethod
    def _create_entry(swimmer, stroke, distance, time):
        return {
            'SwimmerId': swimmer.id,
            'Name': swimmer.name,
            'Birth_Date': swimmer.birth_date.strftime('%Y-%m-%d'),
            'Age': swimmer.age,
            'Gender': swimmer.gender,
            'Citizenship': swimmer.citizenship,
            'Birth_Country': swimmer.birth_country,
            'Residency_Years': swimmer.residency_years,
            'Approving_Authority': swimmer.approving_authority,
            'Education_Level': swimmer.education.level,
            'Criminal_Record': swimmer.criminal_record,
            'Is_Disqualified': swimmer.is_disqualified,
            'Vaccination_Status': swimmer.vaccination_status,
            'Sponsor': swimmer.sponsor,
            'Club': swimmer.club,
            'Region': swimmer.region,
            'Event': f"{distance}m {stroke}",
            'Time': Competition.format_time(time),
            'Historical_Times': [Competition.format_time(t) for t in swimmer.historical_times[stroke][distance]],
            'Is_Personal_Best': time == swimmer.personal_bests[stroke][distance],
            'Is_Valid_Record': swimmer.valid_records[stroke][distance]
        }
    
    @staticmethod
    def _update_rankings(results, swimmers):
        for rank, result in enumerate(results, 1):
            result['Overall_Rank'] = rank
            result['Top_5'] = 'Yes' if rank <= 5 else 'No'
            
            if rank <= 5:
                swimmer = next(s for s in swimmers if s.id == result['SwimmerId'])
                swimmer.rankings[result['Event'].split('m ')[1]][f'rank{rank}'] += 1

def generate_data(output_dir='DataExtraction/raw_data'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    swimmers = [Swimmer(is_core=True) for _ in range(50)]
    swimmers.extend([Swimmer(is_core=False) for _ in range(200)])
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    competitions = Competition.generate_schedule(start_date, end_date)
    
    for comp in competitions:
        competition_swimmers = swimmers[:50] + random.sample(swimmers[50:], k=50)
        all_results = []
        
        for stroke, distances in Config.STROKES.items():
            for distance in distances:
                min_entries, max_entries = Config.COMPETITIONS[comp['level']]['entries']
                entries = random.randint(min_entries, max_entries)
                results = Competition.generate_results(comp, stroke, distance, competition_swimmers, entries)
                all_results.extend(results)
        
        df = pd.DataFrame(all_results)
        for key, value in comp.items():
            df[f'Competition_{key.title()}'] = value
        
        filename = f"{comp['date'].strftime('%Y%m%d')}_{comp['competition_id']}_results.csv"
        df.to_csv(os.path.join(output_dir, filename), index=False)
    
    # Generate rankings summary
    rankings_data = [{
        **{k: getattr(swimmer, k) for k in ['id', 'name', 'age', 'gender', 'citizenship', 'birth_country', 
                                           'residency_years', 'approving_authority', 'criminal_record', 
                                           'is_disqualified', 'vaccination_status', 'sponsor', 'club', 'region']},
        'Birth_Date': swimmer.birth_date.strftime('%Y-%m-%d'),
        'Education_Level': swimmer.education.level,
        'Stroke': stroke,
        **{f'Rank{i}_Count': swimmer.rankings[stroke][f'rank{i}'] for i in range(1, 6)},
        'Historical_Records': swimmer.historical_times[stroke]
    } for swimmer in swimmers for stroke in Config.STROKES]
    
    pd.DataFrame(rankings_data).to_csv(os.path.join(output_dir, 'swimmer_rankings_summary.csv'), index=False)

def main():
    print("Starting data generation...")
    try:
        output_dir = 'DataExtraction/raw_data'
        print(f"Creating output directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        
        print("Generating sample data...")
        # Create a small sample dataset for testing
        data = {
            'SwimmerId': ['SWM001', 'SWM002', 'SWM003'],
            'Name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
            'Birth_Date': ['2000-01-01', '2001-02-02', '1999-03-03'],
            'Education_Level': ['Bachelors', 'High School', 'MBA']
        }
        
        print("Creating DataFrame...")
        df = pd.DataFrame(data)
        
        output_file = os.path.join(output_dir, 'sample_data.csv')
        print(f"Saving data to: {output_file}")
        df.to_csv(output_file, index=False)
        
        print("Generating competition data...")
        generate_data()  # This will generate all the competition data
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main() 