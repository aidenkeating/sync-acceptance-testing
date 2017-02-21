const async = require('async');
const bodyParser = require('body-parser');
const cluster = require('cluster');
const mbaasApi = require('fh-mbaas-api');
const express = require('express');
const mbaasExpress = mbaasApi.mbaasExpress();
const cors = require('cors');

const app = express();

// Try to perform a clean close on shutdown events.
process.on('SIGTERM', close);
process.on('SIGHUP', close);
process.on('INT', close);

// Used to check whether the server should respond.
const serverStatus = {
  crashed: false
};

// Enable CORS for all requests
app.use(cors());

// Use JSON body parser for all requests
app.use(bodyParser.json());

// Check if the server is currently set to crash for all requests.
app.use(function crashIfNeeded(req, res, next) {
  if (req.originalUrl.indexOf('mbaas/sync') !== -1) {
    console.log('worker ', cluster.worker.id);
  }
  if (req.originalUrl.indexOf('mbaas/sync') !== -1 && serverStatus.crashed) {
    return res.status(403).end();
  } else {
    next();
  }
});

// Note: the order which we add middleware to Express here is important!
app.use('/sys', mbaasExpress.sys([]));
app.use('/mbaas', mbaasExpress.mbaas);

// allow serving of static files from the public directory
app.use(express.static(__dirname + '/public'));

// Note: important that this is added just before your own Routes
app.use(mbaasExpress.fhmiddleware());

app.get('/', function(req, res) {
  res.json({ message: 'Hello from worker ' + cluster.worker.id });
});
app.post('/server/status', updateStatus);
app.post('/server/scaleUp', scaleUpCluster);
app.post('/server/scaleDown', scaleDownCluster);
app.post('/datasets/:datasetId/reset', resetDataset);
app.post('/datasets', createDataset);
app.post('/datasets/:datasetId/records', createRecord);
app.put('/datasets/:datasetId/records/:recordId', updateRecord);

// Important that this is last!
app.use(mbaasExpress.errorHandler());

if (cluster.isMaster) {
  const initialClusterSize = process.env.INITIAL_CLUSTER_SIZE || 8;

  // Handle commands sent from workers e.g. `scaleUp` and `scaleDown`.
  cluster.on('message', function(message) {
    if (message.command === 'scaleUp') {
      console.log('Scaling cluster up by ', message.amount);
      scaleUp(message.amount);
    } else if (message.command === 'scaleDown') {
      console.log('Scaling cluster down by ', message.amount);
      scaleDown(message.amount);
    }
  });

  for (var i = 0; i < initialClusterSize; i++) {
    console.log('Creating worker');
    cluster.fork();
  }
} else {
  var port = process.env.FH_PORT || process.env.OPENSHIFT_NODEJS_PORT || 8001;
  var host = process.env.OPENSHIFT_NODEJS_IP || '0.0.0.0';
  app.listen(port, host, function() {});
}

/**
 * Request handler to delete all collections in MongoDB for a provided dataset.
 */
function resetDataset(req, res) {
  // Define titles for default, updates and collision collections in MongoDB.
  const dataset = req.params.datasetId;
  const updates = dataset + '-updates';
  const collisions = dataset + '_collision';

  // Delete all records in the MongoDB collections for the dataset.
  async.forEach([dataset, updates, collisions], function(collection, cb) {
    // If this fails we don't want the test to fail.
    resetCollection(collection, cb);
  }, function(err) {
    if (err) {
      return res.json({error: err }).status(500);
    }
    return res.json({message: 'Dataset ' + dataset + ' reset'}).status(200);
  });
}

/**
 * Remove all records for a particular collection in MongoDB.
 *
 * @param {string} collection - Name of the MongoDB collection.
 * @param {Function} cb - Callback function.
 */
function resetCollection(collection, cb) {
  mbaasApi.db({
    act: 'deleteall',
    type: collection
  }, cb);
}

/**
 * Update a record in a particular dataset. Used to cause a collision.
 */
function updateRecord(req, res) {

  const dataset = req.params.datasetId;
  const record = req.params.recordId;
  const recordData = req.body.data;

  mbaasApi.db({
    act: 'update',
    type: dataset,
    guid: record,
    fields: recordData
  }, function(err, data) {
    if (err) {
      return res.json({ error: err }).status(500);
    }
    return res.json({ data: data }).status(200);
  });
}

/**
 * Create a record in a particular dataset.
 */
function createRecord(req, res) {
  const dataset = req.params.datasetId;
  const recordData = req.body.data;
  mbaasApi.db({
    act: 'create',
    type: dataset,
    fields: recordData
  }, function(err, data) {
    if (err) {
      return res.json({ error: err }).status(500);
    }
    return res.json({ data: data }).status(200);
  });
}

/**
 * Create a dataset with the provided options.
 */
function createDataset(req, res) {
  const datasetName = req.body.name;
  const datasetOptions = req.body.options;

  mbaasApi.sync.init(datasetName, datasetOptions, function(err, dataset) {
    if (err) {
      return res.json({ error: err }).status(500);
    }
    return res.json({ data: dataset }).status(200);
  });
}

/**
 * Update current server status with the options provided.
 * Used to set the server to a crashed state (returning 500's).
 */
function updateStatus(req, res) {
  if (req.body.status.hasOwnProperty('crashed')) {
    serverStatus.crashed = req.body.status.crashed;
  }

  res.json({ data: serverStatus }).status(200);
}

function scaleUpCluster(req, res) {
  console.log('worker', cluster.worker.id);
  const amount = req.body.amount;
  process.send({ command: 'scaleUp', amount: amount });
  res.json({ message: 'Scaling in progress' }).status(200);
}

function scaleDownCluster(req, res) {
  console.log('worker', cluster.worker.id);
  const amount = req.body.amount;
  process.send({ command: 'scaleDown', amount: amount });
  res.json({ message: 'Scaling in progress '}).status(200);
}

/**
 * Return the current number of workers in the cluster.
 *
 * @returns {number} - The number of workers.
 */
function getClusterSize() {
  return Object.keys(cluster.workers).length;
}

/**
 * Scale the cluster up to the amount of workers provided.
 *
 * @param {number} amount - The number of workers to add to the cluster.
 * @param {Function} cb - Callback function which passes the new worker count.
 */
function scaleUp(amount) {
  for (var i = 0; i < amount; i++) {
    cluster.fork();
  }
}

/**
 * Scale the cluster up to the amount of workers provided.
 *
 * @param {number} amount - The number of workers to remove from the cluster.
 * @param {Function} cb - Callback function which passes the new worker count.
 */
function scaleDown(amount) {
  if (getClusterSize() - amount < 1) {
    return process.exit(0);
  }

  for (var i = 0, killed = 0; killed < amount; i++) {
    const workerId = Object.keys(cluster.workers)[i];
    const worker = cluster.workers[workerId];
    killed++;
    worker.kill();
  }
}

function close() {
  if (cluster.isMaster) {
    // Kill all worker processes before exit
    for (var id in cluster.workers) {
      if (cluster.workers[id]) {
        cluster.workers[id].process.kill();
      }
    }
  }
  process.exit(0);
}

module.exports = {
  close: close
};