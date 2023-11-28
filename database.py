import sqlite3


class DatabaseHandler:
    def __init__(self, database_name='your_database.db'):
        self.conn = sqlite3.connect(database_name)
        self.cursor = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        print("Creating tables...")
        # Define table creation SQL statements
        table_creation_sql = [
            '''
            CREATE TABLE IF NOT EXISTS label(
                id INTEGER PRIMARY KEY,
                location TEXT,
                value TEXT,
                entity_id INTEGER REFERENCES Entity(id)
            )
            ''',
            '''
            CREATE TABLE IF NOT EXISTS aliases(
                id INTEGER PRIMARY KEY,
                location TEXT,
                value TEXT,
                entity_id INTEGER REFERENCES Entity(id)
            )
            ''',
            '''
            CREATE TABLE IF NOT EXISTS description(
                id INTEGER PRIMARY KEY,
                location TEXT,
                value TEXT,
                entity_id INTEGER REFERENCES Entity(id)
            )
            ''',
            '''
            CREATE TABLE IF NOT EXISTS Entity (
                id INTEGER PRIMARY KEY,
                location TEXT,
                value TEXT,
                type TEXT,
                datatype TEXT
            );
            ''',
            '''
            CREATE TABLE IF NOT EXISTS Reference (
                id INTEGER PRIMARY KEY,
                property TEXT,
                hash TEXT,
                claim_id INTEGER REFERENCES Claim(id),
                claimId TEXT REFERENCES Claim(claimId)
            );
            ''',
            '''
            CREATE TABLE IF NOT EXISTS Qualifier (
                id INTEGER PRIMARY KEY,
                property TEXT,
                datatype TEXT,
                hash TEXT,
                location TEXT,
                claim_id INTEGER REFERENCES Claim(id),
                claimId TEXT REFERENCES Claim(claimId)
            );
            ''',
            '''
            CREATE TABLE IF NOT EXISTS Claim (
                id INTEGER PRIMARY KEY,
                claimId TEXT,
                property TEXT,
                datatype TEXT,
                rank TEXT,
                location TEXT,
                entity_id INTEGER REFERENCES Entity(id),
                entity_value TEXT REFERENCES Entity(value),
                reference_id INTEGER REFERENCES Reference(id)
            );
            ''',
            '''
            CREATE TABLE IF NOT EXISTS Entityid (
                id INTEGER PRIMARY KEY,
                location TEXT,
                value TEXT,
                entity_id INTEGER REFERENCES Claim(id),
                claim_id INTEGER REFERENCES Claim(id),
                claimId TEXT REFERENCES Claim(claimId),
                reference_id INTEGER REFERENCES Reference(id),
                qualifier_id INTEGER REFERENCES Qualifier(id)
            )
            ''',
              
            '''
            CREATE TABLE IF NOT EXISTS Sitelink (
                id INTEGER PRIMARY KEY,
                site TEXT,
                title TEXT,
                badges TEXT,
                url TEXT,
                entityid_id INTEGER REFERENCES Entityid(id)
            )
            ''',
            '''
            CREATE TABLE IF NOT EXISTS Globecoordinate (
                id INTEGER PRIMARY KEY,
                latitude REAL,
                longitude REAL,
                altitude REAL,
                precision REAL,
                location TEXT,
                globe_id INTEGER REFERENCES Entityid(id),
                claim_id INTEGER REFERENCES Claim(id),
                claimId TEXT REFERENCES Claim(claimId),
                reference_id INTEGER REFERENCES Reference(id),
                qualifier_id INTEGER REFERENCES Qualifier(id)
                
                
            )
            ''',
            '''
            CREATE TABLE IF NOT EXISTS Time (
                id INTEGER PRIMARY KEY,
                time TEXT,
                timezone TEXT,
                before INTEGER,
                after INTEGER,
                precision INTEGER,
                location TEXT,
                calendarmodel INTEGER REFERENCES Entityid(id),
                claim_id INTEGER REFERENCES Claim(id),
                claimId TEXT REFERENCES Claim(claimId),
                reference_id INTEGER REFERENCES Reference(id),
                qualifier_id INTEGER REFERENCES Qualifier(id)
            )
            ''',
            '''
            CREATE TABLE IF NOT EXISTS Quantity (
                id INTEGER PRIMARY KEY,
                amount REAL,
                lower_bound REAL,
                upper_bound REAL,
                unit TEXT,
                location TEXT,
                entity_id INTEGER REFERENCES Entityid(id),
                claim_id INTEGER REFERENCES Claim(id),
                claimId TEXT REFERENCES Claim(claimId),
                reference_id INTEGER REFERENCES Reference(id),
                qualifier_id INTEGER REFERENCES Qualifier(id)
            )
            ''',
            '''
            CREATE TABLE IF NOT EXISTS Monolingualtext (
                id INTEGER PRIMARY KEY,
                text TEXT,
                language TEXT,
                location TEXT,
                claim_id INTEGER REFERENCES Claim(id),
                claimId TEXT REFERENCES Claim(claimId),
                reference_id INTEGER REFERENCES Reference(id),
                qualifier_id INTEGER REFERENCES Qualifier(id)
            )
            ''',
            '''
            CREATE TABLE IF NOT EXISTS Text (
                id INTEGER PRIMARY KEY,
                text TEXT,
                location TEXT,
                claim_id INTEGER REFERENCES Claim(id),
                claimId TEXT REFERENCES Claim(claimId),
                reference_id INTEGER REFERENCES Reference(id),
                qualifier_id INTEGER REFERENCES Qualifier(id)
            )
            '''
        ]
        # Execute table creation SQL statements
        for count, sql_statement in enumerate(table_creation_sql):
            self.cursor.execute(sql_statement)

        # Commit changes
        self.conn.commit()

    def insert_entitys(self, obj,location=None):
        value_id = obj["id"]
        entity_type = obj["type"]
        # entity_pageid = obj["pageid"]
        output = self.cursor.execute('''
        INSERT INTO Entity (location, value, type)
        VALUES (?, ?, ?)''', (location, value_id, entity_type))
        # Get the ID of the last inserted record
        inserted_id = self.cursor.lastrowid
        # print("Inserted record ID:", inserted_id)

        # print sql table form output
        labels = obj["labels"]
        for label in obj["labels"]:
            language = labels[label]["language"]
            value = labels[label]["value"]
            self.cursor.execute('''
            INSERT INTO label (location, value, entity_id)
            VALUES (?, ?, ?)''', (location, value, inserted_id))
        if "aliases" in obj:
            aliases = obj["aliases"]
            for aliase in obj["aliases"]:
                for i in aliases[aliase]:
                    language = i["language"]
                    value = i["value"]
                    self.cursor.execute('''
                    INSERT INTO aliases (location, value, entity_id)
                    VALUES (?, ?, ?)''', (location, value, inserted_id))
        if "descriptions" in obj:
            descriptions = obj["descriptions"]
            for description in obj["descriptions"]:
                language = descriptions[description]["language"]
                value = descriptions[description]["value"]
                self.cursor.execute('''
                INSERT INTO description (location, value, entity_id)
                VALUES (?, ?, ?)''', (location, value, inserted_id))
        if "sitelinks" in obj:
            for sitelink in obj["sitelinks"]:
                site =None
                title =None
                badges =None
                url =None
                if "site" in obj["sitelinks"]:
                    site =obj["sitelinks"][sitelink]["site"]
                if "title" in obj["sitelinks"]:
                    title =obj["sitelinks"][sitelink]["title"]
                if "badges" in obj["sitelinks"]:
                    badges =obj["sitelinks"][sitelink]["badges"].join(",")
                if "url" in obj["sitelinks"]:
                    url =obj["sitelinks"][sitelink]["url"]
                self.cursor.execute('''
                    INSERT INTO Sitelink (site, title, badges, url, entityid_id)
                    VALUES (?, ?, ?, ?, ?)''',
                    (site, title, badges, url, inserted_id))
        
        # print(obj.keys())
        for claim in obj["claims"]:
            for item in obj["claims"][claim]:
                claimId = item["id"]
                self.cursor.execute('''
                INSERT INTO Claim (property, datatype, rank, location, claimId, entity_id)
                VALUES (?, ?, ?, ?, ?, ?)''',
                                    (claim, item["mainsnak"]["datatype"], item["rank"], location, claimId, value_id,))
                # Get the ID of the last inserted record
                claim_id = self.cursor.lastrowid
                if "mainsnak" in item:
                    if "datavalue" in item["mainsnak"]:
                        datavalue = item["mainsnak"]["datavalue"]
                        latitude = None
                        longitude = None
                        altitude = None
                        precision = None
                        globe = None
                        if datavalue["type"] == "globecoordinate":
                                if 'latitude' in datavalue["value"]:
                                    latitude = datavalue["value"]["latitude"]
                                if 'longitude' in datavalue["value"]:
                                    longitude = datavalue["value"]["longitude"]
                                if 'altitude' in datavalue["value"]:
                                    altitude = datavalue["value"]["altitude"]
                                if 'precision' in datavalue["value"]:
                                    precision = datavalue["value"]["precision"]
                                if 'globe' in datavalue["value"]:
                                    globe = datavalue["value"]["globe"]
                                output = self.cursor.execute('''
                                INSERT INTO Globecoordinate (latitude, longitude, altitude, precision, globe_id, location, claimId, claim_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?,?)''', (latitude, longitude, altitude, precision, globe, location, claimId, claim_id))                                # print(dir(output))
                                
                        elif datavalue["type"] == "time":
                                value = datavalue["value"]
                                time = None
                                timezone = None
                                before = None
                                after = None
                                precision = None
                                calendarmodel = None
                                if 'time' in datavalue["value"]:
                                    time = datavalue["value"]["time"]
                                if 'timezone' in datavalue["value"]:
                                    timezone = datavalue["value"]["timezone"]
                                if 'before' in datavalue["value"]:
                                    before = datavalue["value"]["before"]
                                if 'after' in datavalue["value"]:
                                    after = datavalue["value"]["after"]
                                if 'precision' in datavalue["value"]:
                                    precision = datavalue["value"]["precision"]
                                if 'calendarmodel' in datavalue["value"]:
                                    calendarmodel = datavalue["value"]["calendarmodel"]
                                output = self.cursor.execute('''
                                INSERT INTO Time (time, timezone, before, after, precision, calendarmodel,location,claimId, claim_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?,?)''', (time, timezone, before, after, precision, calendarmodel, location, claimId, claim_id))
                                # print(dir(output))
                        elif datavalue["type"] == "quantity":
                                upperBound= None
                                lowerBound= None
                                amount= None
                                unit= None
                                if "value" in datavalue:
                                    if "upperBound" in datavalue["value"]:
                                        upperBound = datavalue["value"]["upperBound"]
                                    if "lowerBound" in datavalue["value"]:
                                        lowerBound = datavalue["value"]["lowerBound"]
                                    if "amount" in datavalue["value"]:
                                        amount = datavalue["value"]["amount"]
                                    if "unit" in datavalue["value"]:
                                        unit = datavalue["value"]["unit"]
                                output = self.cursor.execute('''
                                INSERT INTO Quantity (amount, lower_bound, upper_bound, unit,location,claimId, claim_id)
                                VALUES (?, ?, ?, ?, ?, ?,?)''', (amount, lowerBound, upperBound, unit, location, claimId, claim_id))
                                # print(dir(output))
                                                    
                        elif datavalue["type"] == "monolingualtext":
                                language =None 
                                value =None 
                                if "value" in datavalue:
                                    value = datavalue["value"]['text']
                                if "language" in datavalue:
                                    language = datavalue["value"]["language"]
                                output = self.cursor.execute('''
                                INSERT INTO Monolingualtext (text, language,location,claimId, claim_id)
                                VALUES (?, ?, ?, ?,?)''', (value, language, location, claimId, claim_id))
                                # print(output)
                        elif datavalue["type"] == "string":
                                value = datavalue["value"]
                                output = self.cursor.execute('''
                                INSERT INTO Text (text,location,claimId, claim_id)
                                VALUES (?,?,?,?)''', (value, location, claimId, claim_id))
                                # print(dir(output))
                        elif datavalue["type"] == "wikibase-entityid":
                                entity_id = datavalue["value"]['id']
                                output = self.cursor.execute('''
                                INSERT INTO Entityid (value,location,claimId, claim_id)
                                VALUES (?,?,?,?)''', (entity_id, location, claimId, claim_id))
                                # print(dir(output))
                        else:
                                print("datavalue:", datavalue)
                        
                if "rank" in item:
                        item["rank"]
                if "type" in item:
                        item["type"]
                if "id" in item:
                        item["id"]
                if "qualifiers" in item:
                        for qualifier in item["qualifiers"]:
                            for q in item["qualifiers"][qualifier]:
                                hash_ =q["hash"]
                                property_ =q["property"]
                                datatype =q["datatype"]
                                # datavalue = q["datavalue"]
                                self.cursor.execute('''
                                INSERT INTO Qualifier (property, datatype, hash, claimId, claim_id)
                                VALUES (?, ?, ?, ?, ?)''', (property_, datatype, hash_, claimId, claim_id))  
                                qualifier_id = self.cursor.lastrowid
                                if "datavalue" in q:
                                    datavalue = datavalue = q["datavalue"]
                                    if datavalue["type"] == "globecoordinate":
                                        latitude = None
                                        longitude = None
                                        altitude = None
                                        precision = None
                                        globe = None
                                        if 'latitude' in datavalue["value"]:
                                            latitude = datavalue["value"]["latitude"]
                                        if 'longitude' in datavalue["value"]:
                                            longitude = datavalue["value"]["longitude"]
                                        if 'altitude' in datavalue["value"]:
                                            altitude = datavalue["value"]["altitude"]
                                        if 'precision' in datavalue["value"]:
                                            precision = datavalue["value"]["precision"]
                                        if 'globe' in datavalue["value"]:
                                            globe = datavalue["value"]["globe"]
                                        self.cursor.execute('''
                                        INSERT INTO Globecoordinate (latitude, longitude, altitude, precision, globe_id,qualifier_id)
                                        VALUES (?, ?, ?, ?, ?, ?)''',
                                                            (latitude, longitude, altitude, precision, globe,qualifier_id))
                                        
                                    elif datavalue["type"] == "time":
                                        time = None
                                        timezone = None
                                        before = None
                                        after = None
                                        precision = None
                                        calendarmodel = None
                                        if 'time' in datavalue["value"]:
                                            time = datavalue["value"]["time"]
                                        if 'timezone' in datavalue["value"]:
                                            timezone = datavalue["value"]["timezone"]
                                        if 'before' in datavalue["value"]:
                                            before = datavalue["value"]["before"]
                                        if 'after' in datavalue["value"]:
                                            after = datavalue["value"]["after"]
                                        if 'precision' in datavalue["value"]:
                                            precision = datavalue["value"]["precision"]
                                        if 'calendarmodel' in datavalue["value"]:
                                            calendarmodel = datavalue["value"]["calendarmodel"]
                                        self.cursor.execute('''
                                        INSERT INTO Time (time, timezone, before, after, precision, calendarmodel,qualifier_id)
                                        VALUES (?, ?, ?, ?, ?, ?, ?)''', (time, timezone, before, after, precision, calendarmodel, qualifier_id))
                                    elif datavalue["type"] == "quantity":
                                        upperBound= None
                                        lowerBound= None
                                        amount= None
                                        unit= None
                                        if "upperBound" in datavalue["value"]:
                                            upperBound = datavalue["value"]["upperBound"]
                                        if "lowerBound" in datavalue["value"]:
                                            lowerBound = datavalue["value"]["lowerBound"]
                                        if "amount" in datavalue["value"]:
                                            amount = datavalue["value"]["amount"]
                                        if "unit" in datavalue["value"]:
                                            unit = datavalue["value"]["unit"]
                                        self.cursor.execute('''
                                        INSERT INTO Quantity (amount, lower_bound, upper_bound, unit,qualifier_id)
                                        VALUES (?, ?, ?, ?, ?)''', (amount, lowerBound, upperBound, unit, qualifier_id))
                                    elif datavalue["type"] == "quantity":
                                        value =None
                                        unit =None
                                        upperBound =None
                                        lowerBound =None
                                        if "value" in datavalue["value"]:
                                            value = datavalue["value"]["value"]
                                        if "unit" in datavalue["value"]:
                                            unit = datavalue["value"]["unit"]
                                        if "upperBound" in datavalue["value"]:
                                            upperBound = datavalue["value"]["upperBound"]
                                        if "lowerBound" in datavalue["value"]:
                                            lowerBound = datavalue["value"]["lowerBound"]
                                        self.cursor.execute('''
                                        INSERT INTO Quantity (value, unit, upperBound, lowerBound,qualifier_id)
                                        VALUES (?, ?, ?, ?, ?)''', (value, unit, upperBound, lowerBound, qualifier_id))
                                    elif datavalue["type"] == "string":
                                        value = datavalue["value"]
                                        self.cursor.execute('''
                                        INSERT INTO Text (text,qualifier_id)
                                        VALUES (?, ?)''', (value,qualifier_id))
                                        pass
                                    elif datavalue["type"] == "wikibase-entityid":
                                        entity_id = datavalue["value"]['id']
                                        self.cursor.execute('''
                                        INSERT INTO Entityid (value,qualifier_id)
                                        VALUES (?, ?)''', (entity_id, qualifier_id))
                if "references" in item:
                    for reference in item["references"]:
                        hash_ = reference["hash"]
                        for Ps in reference['snaks']:
                            self.cursor.execute('''
                            INSERT INTO Reference (property, hash, claimId, claim_id)
                            VALUES (?, ?, ?, ?)''', (Ps, hash_, claimId, claim_id))
                            reference_id = self.cursor.lastrowid
                            for P in reference['snaks'][Ps]:
                                if "datavalue" not in P:
                                    continue
                                datavalue = P["datavalue"]
                                if P["datavalue"]["type"] == "globecoordinate":
                                    latitude = None
                                    longitude = None
                                    altitude = None
                                    precision = None
                                    globe = None
                                    if 'latitude' in datavalue["value"]:
                                        latitude = datavalue["value"]["latitude"]
                                    if 'longitude' in datavalue["value"]:
                                        longitude = datavalue["value"]["longitude"]
                                    if 'altitude' in datavalue["value"] and not datavalue["value"]["altitude"] == None:
                                        altitude = datavalue["value"]["altitude"]
                                    if 'precision' in datavalue["value"]:
                                        precision = datavalue["value"]["precision"]
                                    if 'globe' in datavalue["value"]:
                                        globe = datavalue["value"]["globe"]
                                    self.cursor.execute('''
                                    INSERT INTO Globecoordinate (latitude, longitude, altitude, precision, globe_id,reference_id)
                                    VALUES (?, ?, ?, ?, ?,?)''',
                                                        (latitude, longitude, altitude, precision, globe,reference_id))
                                elif P["datavalue"]["type"] == "time":
                                    time = None
                                    timezone = None
                                    before = None
                                    after = None
                                    precision = None
                                    calendarmodel = None
                                    if 'time' in datavalue["value"]:
                                        time = datavalue["value"]["time"]
                                    if 'timezone' in datavalue["value"]:
                                        timezone = datavalue["value"]["timezone"]
                                    if 'before' in datavalue["value"]:
                                        before = datavalue["value"]["before"]
                                    if 'after' in datavalue["value"]:
                                        after = datavalue["value"]["after"]
                                    if 'precision' in datavalue["value"]:
                                        precision = datavalue["value"]["precision"]
                                    if 'calendarmodel' in datavalue["value"]:
                                        calendarmodel = datavalue["value"]["calendarmodel"]
                                    self.cursor.execute('''
                                    INSERT INTO Time (time, timezone, before, after, precision, calendarmodel,reference_id)
                                    VALUES (?, ?, ?, ?, ?, ?,?)''',(time, timezone, before, after, precision, calendarmodel,reference_id))
                                elif P["datavalue"]["type"] == "quantity":
                                    upperBound= None
                                    lowerBound= None
                                    amount= None
                                    unit= None
                                    if "upperBound" in datavalue["value"]:
                                        upperBound = datavalue["value"]["upperBound"]
                                    if "lowerBound" in datavalue["value"]:
                                        lowerBound = datavalue["value"]["lowerBound"]
                                    if "amount" in datavalue["value"]:
                                        amount = datavalue["value"]["amount"]
                                    if "unit" in datavalue["value"]:
                                        unit = datavalue["value"]["unit"]
                                    self.cursor.execute('''
                                    INSERT INTO Quantity (amount, lower_bound, upper_bound, unit,reference_id)
                                    VALUES (?, ?, ?, ?,?)''',(amount, lowerBound, upperBound, unit,reference_id))
                                elif P["datavalue"]["type"] == "monolingualtext":
                                    value =None
                                    language =None
                                    if "value" in datavalue["value"]:
                                        value = datavalue["value"] 
                                    if "language" in datavalue["value"]:
                                        language = datavalue["value"]["language"]
                                    self.cursor.execute('''
                                    INSERT INTO Monolingualtext (text, language,reference_id)
                                    VALUES (?, ?,?)''', (value, language,reference_id))
                                elif P["datavalue"]["type"] == "string":
                                    value = datavalue["value"]
                                    self.cursor.execute('''
                                    INSERT INTO Text (text,reference_id)
                                    VALUES (?,?)''', (value,reference_id,))
                                elif P["datavalue"]["type"] == "wikibase-entityid":
                                    entity_id = datavalue["value"]['id']
                                    self.cursor.execute('''
                                    INSERT INTO Entityid (value,reference_id)
                                    VALUES (?,?)''', (entity_id,reference_id,))
        # self.conn.commit()

    def process_qualifiers(qualifiers):
        print("Processing qualifiers...")
        output = []
        for qualifier in qualifiers:
            for item_ in qualifiers[qualifier]:
                output.append(item_)

    def close_connection(self):
        print("Closing connection...")
        # Close the database connection
        self.conn.close()

