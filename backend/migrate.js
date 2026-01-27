const { Pool } = require('pg')
const fs = require('fs')
const path = require('path')

const databaseUrl = 'postgresql://neondb_owner:npg_5lBiKSDN2ZnT@ep-empty-wind-a1vittzf-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'

const pool = new Pool({
  connectionString: databaseUrl
})

// Create migrations table to track which migrations have been run
async function createMigrationsTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS migrations (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `)
    console.log('✅ Migrations table ready')
  } catch (error) {
    console.error('Error creating migrations table:', error)
    throw error
  }
}

// Get list of migration files
function getMigrationFiles() {
  const migrationsDir = path.join(__dirname, 'migrations')
  if (!fs.existsSync(migrationsDir)) {
    console.error('❌ Migrations directory not found')
    process.exit(1)
  }
  
  const files = fs.readdirSync(migrationsDir)
    .filter(file => file.endsWith('.sql'))
    .sort()
  
  return files.map(file => ({
    name: file,
    path: path.join(migrationsDir, file)
  }))
}

// Check if migration has been executed
async function isMigrationExecuted(migrationName) {
  const result = await pool.query(
    'SELECT * FROM migrations WHERE name = $1',
    [migrationName]
  )
  return result.rows.length > 0
}

// Mark migration as executed
async function markMigrationExecuted(migrationName) {
  await pool.query(
    'INSERT INTO migrations (name) VALUES ($1)',
    [migrationName]
  )
}

// Run a single migration
async function runMigration(migration) {
  const sql = fs.readFileSync(migration.path, 'utf8')
  
  try {
    console.log(`🔄 Running migration: ${migration.name}`)
    await pool.query(sql)
    await markMigrationExecuted(migration.name)
    console.log(`✅ Migration ${migration.name} completed successfully`)
  } catch (error) {
    console.error(`❌ Error running migration ${migration.name}:`, error.message)
    throw error
  }
}

// Main migration function
async function runMigrations() {
  try {
    console.log('🚀 Starting database migrations...\n')
    
    // Create migrations table if it doesn't exist
    await createMigrationsTable()
    
    // Get all migration files
    const migrations = getMigrationFiles()
    
    if (migrations.length === 0) {
      console.log('⚠️  No migration files found')
      return
    }
    
    console.log(`📋 Found ${migrations.length} migration(s)\n`)
    
    // Run each migration that hasn't been executed
    for (const migration of migrations) {
      const executed = await isMigrationExecuted(migration.name)
      
      if (executed) {
        console.log(`⏭️  Skipping ${migration.name} (already executed)`)
      } else {
        await runMigration(migration)
      }
    }
    
    console.log('\n✅ All migrations completed successfully!')
  } catch (error) {
    console.error('\n❌ Migration failed:', error)
    process.exit(1)
  } finally {
    await pool.end()
  }
}

// Run migrations
runMigrations()
