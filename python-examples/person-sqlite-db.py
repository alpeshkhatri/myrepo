import sqlite3
from dataclasses import dataclass, fields
from typing import Optional, List
import json

@dataclass
class Person:
    name: str
    age: int
    email: str
    id: Optional[int] = None
    
    def to_dict(self):
        """Convert dataclass to dictionary"""
        return {field.name: getattr(self, field.name) for field in fields(self)}
    
    @classmethod
    def from_dict(cls, data):
        """Create dataclass instance from dictionary"""
        return cls(**data)

class PersonDB:
    def __init__(self, db_path: str = "people.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize the database and create table"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS people (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    email TEXT UNIQUE NOT NULL
                )
            ''')
            conn.commit()
    
    def create(self, person: Person) -> Person:
        """Insert a new person and return with assigned ID"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'INSERT INTO people (name, age, email) VALUES (?, ?, ?)',
                (person.name, person.age, person.email)
            )
            person.id = cursor.lastrowid
            conn.commit()
        return person
    
    def get(self, person_id: int) -> Optional[Person]:
        """Get a person by ID"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('SELECT * FROM people WHERE id = ?', (person_id,))
            row = cursor.fetchone()
            if row:
                return Person(**dict(row))
        return None
    
    def get_all(self) -> List[Person]:
        """Get all people"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('SELECT * FROM people')
            return [Person(**dict(row)) for row in cursor.fetchall()]
    
    def update(self, person: Person) -> bool:
        """Update an existing person"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'UPDATE people SET name = ?, age = ?, email = ? WHERE id = ?',
                (person.name, person.age, person.email, person.id)
            )
            conn.commit()
            return cursor.rowcount > 0
    
    def delete(self, person_id: int) -> bool:
        """Delete a person by ID"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('DELETE FROM people WHERE id = ?', (person_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def find_by_email(self, email: str) -> Optional[Person]:
        """Find a person by email"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('SELECT * FROM people WHERE email = ?', (email,))
            row = cursor.fetchone()
            if row:
                return Person(**dict(row))
        return None

# Example usage
if __name__ == "__main__":
    # Initialize database
    db = PersonDB()
    
    # Create new people
    alice = Person("Alice Smith", 30, "alice@example.com")
    bob = Person("Bob Jones", 25, "bob@example.com")
    
    # Insert into database
    alice = db.create(alice)
    bob = db.create(bob)
    
    print(f"Created Alice with ID: {alice.id}")
    print(f"Created Bob with ID: {bob.id}")
    
    # Read from database
    retrieved_alice = db.get(alice.id)
    print(f"Retrieved: {retrieved_alice}")
    
    # Update
    alice.age = 31
    db.update(alice)
    print(f"Updated Alice's age to {alice.age}")
    
    # Get all people
    all_people = db.get_all()
    print(f"All people: {all_people}")
    
    # Find by email
    found_bob = db.find_by_email("bob@example.com")
    print(f"Found by email: {found_bob}")
    
    # Delete
    db.delete(bob.id)
    print(f"Deleted Bob (ID: {bob.id})")
    
    # Verify deletion
    remaining_people = db.get_all()
    print(f"Remaining people: {remaining_people}")
