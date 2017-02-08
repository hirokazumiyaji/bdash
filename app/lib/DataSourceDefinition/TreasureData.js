import td from 'td';
import Base from './Base';
import { flatten } from 'lodash';

class TreasureDataClient {
  constructor(apiKey) {
    this.client = new td(apiKey);
  }

  hiveQuery(database, query, cancel) {
    return new Promise((resolve, reject) => {
      this.client.hiveQuery(database, query, (err, results) => {
        if (err) return reject(err);
        cancel = () => {
          this.kill(results.job_id).then(() => reject(new Error('Query is canceled')));
        };

        this.waitJob(this.showJob(results.job_id))
          .then((waitResults) => {
            const fields = JSON.parse(waitResults.hive_result_schema).map((field) => {
              return field[0];
            });
            this.client.jobResult(results.job_id, 'json', (jobResultErr, jobResults) => {
              if (jobResultErr) return reject(JobResultErr);
              const rows = jobResults.split("\n")
                .filter((row) => {
                  return row != '';
                })
                .map((row) => {
                  return JSON.parse(row);
                });
              resolve({
                fields: fields,
                rows: rows,
              });
            });
          })
          .catch((err) => reject(err));
      });
    });
  }

  prestoQuery(database, query, cancel) {
    return new Promise((resolve, reject) => {
      this.client.prestoQuery(database, query, (err, results) => {
        if (err) return reject(err);
        cancel = () => {
          this.kill(results.job_id).then(() => reject(new Error('Query is canceled')));
        };

        this.waitJob(this.showJob(results.job_id))
          .then((waitResults) => {
            const fields = JSON.parse(waitResults.hive_result_schema).map((field) => {
              return field[0];
            });
            this.client.jobResult(results.job_id, 'json', (jobResultErr, jobResults) => {
              if (jobResultErr) return reject(JobResultErr);
              const rows = jobResults.split("\n")
                .filter((row) => {
                  return row != '';
                })
                .map((row) => {
                  return JSON.parse(row);
                });
              resolve({
                fields: fields,
                rows: rows,
              });
            });
          })
          .catch((err) => reject(err));
      });
    });
  }

  listTables(database) {
    return new Promise((resolve, reject) => {
      this.client.listTables(database, (err, results) => {
        if (err) return reject(err);
        resolve(results);
      });
    });
  }

  kill(jobId) {
    return new Promise((resolve, reject) => {
      this.client.kill(jobId, (err, results) => {
        if (err) return reject(err);
        this.waitJob(this.showJob(jobId))
          .then(results => resolve(results))
          .catch(err => reject(err));
      });
    });
  }

  showJob(jobId) {
    return new Promise((resolve, reject) => {
      this.client.showJob(jobId, (err, results) => {
        if (err) return reject(err);
        resolve(results);
      });
    });
  }

  waitJob(promise) {
    return promise.then((results) => {
      if (results.status == 'success' || results.status == 'error') {
        return results;
      }
      return this.wait(1000).then(() => {
        return this.waitJob(this.showJob(results.job_id));
      });
    });
  }

  wait(delay) {
    return new Promise((resolve, reject) => {
      setTimeout(resolve, delay);
    });
  }
}

export default class TreasureData extends Base {
  static get key() { return 'treasuredata'; }
  static get label() { return 'TreasureData'; }
  static get configSchema() {
    return [
      { name: 'api_key', label: 'API Key', type: 'string', placeholder: 'your-api-key', required: true },
      { name: 'engine', label: 'QueryEngine', type: 'string', placeholder: 'presto or hive (default presto)', },
      { name: 'database', label: 'Database', type: 'string', placeholder: 'target database', required: true },
    ];
  }

  execute(query) {
    this._cancel = null;
    return new Promise((resolve, reject) => {
      const client = new TreasureDataClient(this.config.api_key);

      if (this.config.engine == 'hive') {
        client.hiveQuery(this.config.database, query, this._cancel)
          .then((results) => {
            return resolve({
              fields: [],
              rows: [],
            });
          })
          .catch(err => reject(err));
      } else {
        client.prestoQuery(this.config.database, query, this._cancel)
          .then((results) => {
            return resolve(results);
          })
          .catch(err => reject(err));
      }
    });
  }

  cancel() {
    return this._cancel && this._cancel();
  }

  connectionTest() {
    return true;
  }

  fetchTables() {
    return new Promise((resolve, reject) => {
      const client = new TreasureDataClient(this.config.api_key);
      client.listTables(this.config.database)
        .then((results) => {
          resolve(results.tables.map(table => {
            return {
              name: table.name,
              columns: JSON.parse(table.schema),  // [[name, type], ...]
              count: table.count,
              type: 'BASE TABLE',
            }
          }));
        })
        .catch(err => reject(err));
    });
  }

  fetchTableSummary({ name, columns, count }) {
    const defs = {
      fields: ["Field", "Type"],
      rows: columns,
    };
    return new Promise((resolve, reject) => {
      resolve({ name, defs });
    });
  }
}
