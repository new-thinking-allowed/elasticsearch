const util = require('util');
const mysql = require('mysql');
const { Client } = require('@elastic/elasticsearch')

const indexStructure = {
  "settings": {
    "number_of_shards": 1
  },
  "mappings": {
    "properties": {
      "id": {
        "type": "text"
      },
      "channel_id": {
        "type": "text"
      },
      "wp_post_id": {
        "type": "text"
      },
      "title": {
        "type": "text"
      },
      "guest": {
        "type": "text"
      },
      "publishedAt": {
        "type": "date"
      },
      "description": {
        "type": "text"
      },
      "thumbnail": {
        "type": "text",
        "index": false
      },
      "playlist": {
        "type": "text"
      },
      "keywords": {
        "type": "text"
      },
      "tags": {
        "type": "text"
      },
      "md5": {
        "type": "text",
        "index": false
      }
    }
  }
};

const INDEX_NAME = 'videos';

const dbConfig = {
  host: process.env.NTA_DB_HOST,
  port: process.env.NTA_DB_PORT,
  user: process.env.NTA_DB_USER,
  password: process.env.NTA_DB_PASS,
  database: process.env.NTA_DB_NAME,
  charset: 'utf8mb4',
};

async function createIndex(es) {
  try {
    console.log("Remove old index ", INDEX_NAME);
    await es.indices.delete({ index: INDEX_NAME });
    console.log("Create new index ", INDEX_NAME);
    await es.indices.create({
      index: INDEX_NAME,
      body: indexStructure,
    });
    console.log("Created new index ", INDEX_NAME);
  } catch (e) {
    throw e;
  }
}

function connect(config) {
  const dbh = mysql.createConnection(config);
  return {
    pause(...args) {
      return dbh.pause(...args);
    },
    resume(...args) {
      return dbh.resume(...args);
    },
    query(...args) {
      return dbh.query(...args);
    },
    asyncQuery(sql, args) {
      return util.promisify(dbh.query)
        .call(dbh, sql, args);
    },
    close() {
      return util.promisify(dbh.end).call(dbh);
    }
  };
}

async function processVideos(es, dbh) {
  console.info('Processing videos');
  let done = 0;

  const query = await dbh.query('SELECT * FROM video');
  query.on('error', (err) => {
    console.error(err);
  })
    .on('fields', (fields) => {
      // the field packets for the rows to follow
    })
    .on('result', async (row) => {
      // Pausing the connnection is useful if your processing involves I/O
      dbh.pause();
      await processVideo(es, row);
      dbh.resume();
      done++;

      if (done % 25 == 0) {
        console.log('  Processed ', done);
      }

    })
    .on('end', () => {
      console.info('All rows processed.');
    });
}

async function processVideo(es, doc) {
  await es.index({
    index: INDEX_NAME,
    body: doc,
    id: doc.id
  });
}


async function main() {
  const dbh = connect(dbConfig);
  const es = new Client({ node: 'http://localhost:9200' })

  try {
    await createIndex(es);
    await processVideos(es, dbh);
  } catch (err) {
    console.error(err);
  } finally {
    dbh.close();
  }
}

main();
