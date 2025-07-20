import sqlite3
from dataclasses import dataclass, fields
from typing import List, Optional
from datetime import datetime

@dataclass
class Person:
    name: str
    age: int
    email: str
    created_at: Optional[datetime] = None
    id: Optional[int] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()

class PersonDatabase:
    def __init__(self, db_path: str = "people.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database and create the table if it doesn't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS people (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
    
    def insert_person(self, person: Person) -> int:
        """Insert a person into the database and return the ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO people (name, age, email, created_at)
                VALUES (?, ?, ?, ?)
            """, (person.name, person.age, person.email, person.created_at))
            conn.commit()
            person.id = cursor.lastrowid
            return cursor.lastrowid
    
    def get_person_by_id(self, person_id: int) -> Optional[Person]:
        """Retrieve a person by ID."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM people WHERE id = ?
            """, (person_id,))
            row = cursor.fetchone()
            
            if row:
                return Person(
                    id=row['id'],
                    name=row['name'],
                    age=row['age'],
                    email=row['email'],
                    created_at=datetime.fromisoformat(row['created_at'])
                )
            return None
    
    def get_all_people(self) -> List[Person]:
        """Retrieve all people from the database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM people ORDER BY name")
            rows = cursor.fetchall()
            
            people = []
            for row in rows:
                people.append(Person(
                    id=row['id'],
                    name=row['name'],
                    age=row['age'],
                    email=row['email'],
                    created_at=datetime.fromisoformat(row['created_at'])
                ))
            return people
    
    def update_person(self, person: Person) -> bool:
        """Update a person in the database."""
        if person.id is None:
            return False
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                UPDATE people 
                SET name = ?, age = ?, email = ?
                WHERE id = ?
            """, (person.name, person.age, person.email, person.id))
            conn.commit()
            return cursor.rowcount > 0
    
    def delete_person(self, person_id: int) -> bool:
        """Delete a person from the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM people WHERE id = ?", (person_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def find_people_by_age_range(self, min_age: int, max_age: int) -> List[Person]:
        """Find people within a specific age range."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM people 
                WHERE age BETWEEN ? AND ? 
                ORDER BY age
            """, (min_age, max_age))
            rows = cursor.fetchall()
            
            people = []
            for row in rows:
                people.append(Person(
                    id=row['id'],
                    name=row['name'],
                    age=row['age'],
                    email=row['email'],
                    created_at=datetime.fromisoformat(row['created_at'])
                ))
            return people

# Example usage
def main():
    # Initialize the database
    db = PersonDatabase()
    
    # Create some sample people
    people = [
        Person("Alice Johnson", 28, "alice@example.com"),
        Person("Bob Smith", 35, "bob@example.com"),
        Person("Charlie Brown", 42, "charlie@example.com"),
        Person("Diana Prince", 30, "diana@example.com")
    ]
    
    # Insert people into the database
    print("Inserting people...")
    for person in people:
        person_id = db.insert_person(person)
        print(f"Inserted {person.name} with ID: {person_id}")
    
    print("\n" + "="*50)
    
    # Retrieve and display all people
    print("All people in database:")
    all_people = db.get_all_people()
    for person in all_people:
        print(f"ID: {person.id}, Name: {person.name}, Age: {person.age}, "
              f"Email: {person.email}, Created: {person.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("\n" + "="*50)
    
    # Get a specific person by ID
    person = db.get_person_by_id(2)
    if person:
        print(f"Person with ID 2: {person.name}, {person.age} years old")
    
    # Update a person
    if person:
        person.age = 36
        if db.update_person(person):
            print(f"Updated {person.name}'s age to {person.age}")
    
    print("\n" + "="*50)
    
    # Find people in age range
    print("People aged 30-40:")
    age_filtered = db.find_people_by_age_range(30, 40)
    for person in age_filtered:
        print(f"- {person.name}, {person.age} years old")
    
    print("\n" + "="*50)
    
    # Delete a person
    if db.delete_person(3):
        print("Deleted person with ID 3")
    
    # Show remaining people
    print("\nRemaining people:")
    remaining_people = db.get_all_people()
    for person in remaining_people:
        print(f"- {person.name}")

if __name__ == "__main__":
    main()
