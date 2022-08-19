const express = require("express");
const { open } = require("sqlite");
const sqlite3 = require("sqlite3");
const path = require("path");

const app = express();
app.use(express.json()); // To parse JSON data in request body

const covid19IndiaDataFilePath = path.join(__dirname, "covid19India.db");
const sqliteDBDriver = sqlite3.Database;

let covid19IndiaDBConnectionObj = null;

const initializeDBAndServer = async () => {
  try {
    covid19IndiaDBConnectionObj = await open({
      filename: covid19IndiaDataFilePath,
      driver: sqliteDBDriver,
    });

    app.listen(3000, () => {
      console.log("Server running and listening on port 3000 !");
    });
  } catch (exception) {
    console.log(`Error initializing DB or Server: ${exception.message}`);
    process.exit(1);
  }
};

initializeDBAndServer();

/*
    End-Point 1: GET /states
    ------------
    To all states data from the
    state table in sqlite database
*/

app.get("/states", async (req, res) => {
  const getAllStatesQuery = `
    SELECT 
        *
    FROM
        state;
    `;

  const allStatesData = await covid19IndiaDBConnectionObj.all(
    getAllStatesQuery
  );
  const processedStatesData = allStatesData.map((singleStateData) => ({
    stateId: singleStateData.state_id,
    stateName: singleStateData.state_name,
    population: singleStateData.population,
  }));

  res.send(processedStatesData);
});

/*
    End-Point 2: GET /states/:stateId
    -----------
    To fetch specific state data with
    id: stateId from state table in
    sqlite database
*/
app.get("/states/:stateId", async (req, res) => {
  const { stateId } = req.params;

  const getSpecificStateDataQuery = `
    SELECT
        *
    FROM
        state
    WHERE
        state_id = ${stateId};
    `;

  const specificStateData = await covid19IndiaDBConnectionObj.get(
    getSpecificStateDataQuery
  );
  const processedSpecificStateData = {
    stateId: specificStateData.state_id,
    stateName: specificStateData.state_name,
    population: specificStateData.population,
  };

  res.send(processedSpecificStateData);
});

module.exports = app;
