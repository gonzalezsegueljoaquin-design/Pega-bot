#!/usr/bin/env python3
"""
Script de migración DB - Agregar columnas faltantes
Ejecutar UNA VEZ antes del bot v6
"""

import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")

def migrate():
    print("🔧 INICIANDO MIGRACIÓN DE BASE DE DATOS")
    print("="*60)
    
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    cur = conn.cursor()
    
    try:
        # 1. Verificar si columnas existen
        print("\n1️⃣ Verificando columnas existentes...")
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'jobs'
        """)
        existing_columns = [row[0] for row in cur.fetchall()]
        print(f"   Columnas actuales: {', '.join(existing_columns)}")
        
        # 2. Agregar columna requisitos si no existe
        if 'requisitos' not in existing_columns:
            print("\n2️⃣ Agregando columna 'requisitos' (JSONB)...")
            cur.execute("ALTER TABLE jobs ADD COLUMN requisitos JSONB DEFAULT '[]'::jsonb")
            print("   ✅ Columna 'requisitos' agregada")
        else:
            print("\n2️⃣ Columna 'requisitos' ya existe ✓")
        
        # 3. Agregar columna resumen si no existe
        if 'resumen' not in existing_columns:
            print("\n3️⃣ Agregando columna 'resumen' (TEXT)...")
            cur.execute("ALTER TABLE jobs ADD COLUMN resumen TEXT DEFAULT 'No disponible'")
            print("   ✅ Columna 'resumen' agregada")
        else:
            print("\n3️⃣ Columna 'resumen' ya existe ✓")
        
        # 4. Commit
        conn.commit()
        print("\n" + "="*60)
        print("✅ MIGRACIÓN COMPLETADA EXITOSAMENTE")
        print("="*60)
        
        # 5. Verificar schema final
        print("\n📊 SCHEMA FINAL:")
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'jobs'
            ORDER BY ordinal_position
        """)
        for col, dtype in cur.fetchall():
            print(f"   • {col}: {dtype}")
        
        print("\n✅ La base de datos está lista para bot v6")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ ERROR: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    if not DATABASE_URL:
        print("❌ ERROR: Falta DATABASE_URL")
        exit(1)
    migrate()
