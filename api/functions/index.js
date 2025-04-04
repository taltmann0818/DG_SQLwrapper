const sql = require('mssql');

module.exports = async function (context, req) {
  try {
    // Check if we have data to process
    if (!req.body || !Array.isArray(req.body)) {
      context.res = {
        status: 400,
        body: { error: "Request body must be an array of records" }
      };
      return;
    }

    const records = req.body;
    
    // Get connection string from environment variables
    // (Set this in your Azure portal or local.settings.json for local dev)
    const config = {
      server: process.env.SQL_SERVER,
      database: process.env.SQL_DATABASE,
      user: process.env.SQL_USER,
      password: process.env.SQL_PASSWORD,
      options: {
        encrypt: true
      }
    };

    // Connect to database
    await sql.connect(config);
    
    // Start transaction
    const transaction = new sql.Transaction();
    await transaction.begin();
    
    try {
      // Create a request object linked to transaction
      const request = new sql.Request(transaction);
      
      // Insert records
      let insertCount = 0;
      for (const record of records) {
        await request.query(`
          INSERT INTO nasdaq_1day (Date, Open, High, Low, Close, Volume, Ticker)
          VALUES ('${record.Date}', ${record.Open}, ${record.High}, 
                 ${record.Low}, ${record.Close}, ${record.Volume}, '${record.Ticker}')
        `);
        insertCount++;
      }
      
      // Commit the transaction
      await transaction.commit();
      
      context.res = {
        status: 200,
        body: { 
          success: true, 
          message: `Successfully inserted ${insertCount} records` 
        }
      };
    } catch (err) {
      // If error occurs, roll back
      await transaction.rollback();
      throw err;
    }
  } catch (error) {
    context.log.error('Database error:', error);
    context.res = {
      status: 500,
      body: { 
        error: "Database operation failed", 
        details: error.message 
      }
    };
  } finally {
    sql.close();
  }
};