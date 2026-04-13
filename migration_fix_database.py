"""
Script de Migración - Agregar columnas faltantes a la base de datos
Ejecutar esto en Railway para corregir la estructura de la base de datos
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = os.getenv("PERSISTENT_STORAGE_PATH", ".").strip() + "/buen_samaritano.db"

def migrate_database():
    """Agrega las columnas faltantes a la base de datos existente"""
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            
            print("🔧 Iniciando migración de base de datos...")
            
            # 1. Verificar si la columna references_count existe
            c.execute("PRAGMA table_info(users)")
            columns = [col[1] for col in c.fetchall()]
            
            if 'references_count' not in columns:
                print("  ➕ Agregando columna 'references_count' a tabla 'users'...")
                c.execute("ALTER TABLE users ADD COLUMN references_count INTEGER DEFAULT 0")
                print("  ✅ Columna 'references_count' agregada")
            else:
                print("  ℹ️  Columna 'references_count' ya existe")
            
            # 2. Verificar si la tabla user_references existe
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_references'")
            if not c.fetchone():
                print("  ➕ Creando tabla 'user_references'...")
                c.execute("""
                    CREATE TABLE user_references (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        file_id TEXT,
                        message_id INTEGER,
                        published_at TEXT,
                        FOREIGN KEY (user_id) REFERENCES users(tg_id)
                    )
                """)
                print("  ✅ Tabla 'user_references' creada")
            else:
                print("  ℹ️  Tabla 'user_references' ya existe")
            
            # 3. Verificar si la tabla blacklist existe
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='blacklist'")
            if not c.fetchone():
                print("  ➕ Creando tabla 'blacklist'...")
                c.execute("""
                    CREATE TABLE blacklist (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER UNIQUE,
                        reason TEXT,
                        banned_by_admin_id INTEGER,
                        banned_at TEXT,
                        FOREIGN KEY (user_id) REFERENCES users(tg_id)
                    )
                """)
                print("  ✅ Tabla 'blacklist' creada")
            else:
                print("  ℹ️  Tabla 'blacklist' ya existe")
            
            # 4. Verificar si la tabla ban_evidence existe
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ban_evidence'")
            if not c.fetchone():
                print("  ➕ Creando tabla 'ban_evidence'...")
                c.execute("""
                    CREATE TABLE ban_evidence (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        file_id TEXT,
                        reason TEXT,
                        created_at TEXT,
                        FOREIGN KEY (user_id) REFERENCES blacklist(user_id)
                    )
                """)
                print("  ✅ Tabla 'ban_evidence' creada")
            else:
                print("  ℹ️  Tabla 'ban_evidence' ya existe")
            
            # Confirmar cambios
            conn.commit()
            print("\n✅ Migración completada exitosamente")
            print(f"📁 Base de datos: {DB_PATH}")
            
            # Mostrar estructura final
            print("\n📋 Estructura final de la base de datos:")
            c.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = c.fetchall()
            for table in tables:
                print(f"  - {table[0]}")
            
            return True
            
    except sqlite3.OperationalError as e:
        if "already exists" in str(e):
            print(f"⚠️  Advertencia: {e}")
            print("   Esto es normal si la migración ya fue ejecutada.")
            return True
        else:
            print(f"❌ Error de base de datos: {e}")
            return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("SCRIPT DE MIGRACIÓN - SAMARITAN BOT")
    print("=" * 60)
    print()
    
    success = migrate_database()
    
    print()
    if success:
        print("✅ La base de datos está lista para usar")
        print("\nPróximos pasos:")
        print("1. Desplegar el código actualizado en Railway")
        print("2. Reiniciar el bot")
        print("3. Los comandos /info, /ban, /refe deberían funcionar")
    else:
        print("❌ Hubo un error en la migración")
        print("   Contacta al administrador")
    
    print()
    print("=" * 60)
