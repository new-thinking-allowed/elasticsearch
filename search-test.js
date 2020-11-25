const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' });

async function test() {
  try {
    const result = await client.search({
      index: 'videos',
      body: {
        query: {
          multi_match: {
            query: 'jason',
            fields: [
              'title^5', 'description', 'guest^10', 'tags', 'captions^2'
            ]
          }
        },
        // "_source": false, // https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html#specify-highlight-query
        "_source": ["title", "description", "guest"],
        highlight: {
          fields: {
            title: {},
            desecription: {},
            guest: {},
            captions: {},
            tags: {},
          }
        }
      }
    });

    console.log(JSON.stringify(
      result.body.hits, {}, 4
    ));

  } catch (e) {
    console.error(JSON.stringify(
      e, {}, 4
    ));
  }
}

test();