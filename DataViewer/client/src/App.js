import './App.css';
import React, {Component} from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';

class App extends Component {

  handleStream(e) {
    const event = JSON.parse(e.data);

    const data = this.state.data;


    const key = event['key'];
    const value = event['value'];

    if (key != 'www.wikidata.org') {
      return;
    }

    if (data[key] === undefined) {
      const newKey = {};
      newKey['new'] = {};
      newKey['new']['hour'] = 0;
      newKey['new']['day'] = 0;
      newKey['new']['week'] = 0;
      newKey['new']['month'] = 0;

      newKey['edit'] = {};
      newKey['edit']['hour'] = 0;
      newKey['edit']['day'] = 0;
      newKey['edit']['week'] = 0;
      newKey['edit']['month'] = 0;

      newKey['revert'] = {};
      newKey['revert']['hour'] = 0;
      newKey['revert']['day'] = 0;
      newKey['revert']['week'] = 0;
      newKey['revert']['month'] = 0;

      newKey['topUsers'] = {};
      newKey['topUsers']['hour'] = 0;
      newKey['topUsers']['day'] = 0;
      newKey['topUsers']['week'] = 0;
      newKey['topUsers']['month'] = 0;

      newKey['topBots'] = {};
      newKey['topBots']['hour'] = 0;
      newKey['topBots']['day'] = 0;
      newKey['topBots']['week'] = 0;
      newKey['topBots']['month'] = 0;

      newKey['topPages'] = {};
      newKey['topPages']['hour'] = 0;
      newKey['topPages']['day'] = 0;
      newKey['topPages']['week'] = 0;
      newKey['topPages']['month'] = 0;

      data[key] = newKey;
    }

    if (value['meta']['type'] === 'top') {
      if (value['meta']['bot'] === true) {
        data[key]['topBots'][value['meta']['timeFrame']] = value;
      }
      if (value['meta']['bot'] === false) {
        data[key]['topUsers'][value['meta']['timeFrame']] = value;
      }
    }
    else if (value['meta']['type'] === 'topPage') {
      data[key]['topPages'][value['meta']['timeFrame']] = value;
    }
    else if (value['meta']['type'] === 'stats') {
      data[key][value['type']][value['meta']['timeFrame']] = value;
    }

    
    this.setState({data: data});

  }

  constructor (props) {
    super(props);
    this.state = {
      data: {}
    };
  }

  componentDidMount() {
    const sse = new EventSource('http://localhost:8000/stream');
    
    sse.onmessage = e => {this.handleStream(e)};

    sse.onerror = e => {
      console.log(e);
      sse.close();
    };
  }

    renderStats = (statsData) => {

    const hour = statsData['hour'];
    const day = statsData['day'];
    const week = statsData['week'];
    const month = statsData['month'];

    return (
      <div>
        <table className="table">
            <thead>
              <tr>
                <th scope="col">Time Frame</th>
                <th scope="col">User</th>
                <th scope="col">User %</th>
                <th scope="col">Bot</th>
                <th scope="col">Bot %</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>hour</td>
                <td>{hour['user']}</td>
                <td>{hour['userRelative']}</td>
                <td>{hour['bot']}</td>
                <td>{hour['botRelative']}</td>
              </tr>
              <tr>
                <td>day</td>
                <td>{day['user']}</td>
                <td>{day['userRelative']}</td>
                <td>{day['bot']}</td>
                <td>{day['botRelative']}</td>
              </tr>
              <tr>
                <td>week</td>
                <td>{week['user']}</td>
                <td>{week['userRelative']}</td>
                <td>{week['bot']}</td>
                <td>{week['botRelative']}</td>
              </tr>
              <tr>
                <td>month</td>
                <td>{month['user']}</td>
                <td>{month['userRelative']}</td>
                <td>{month['bot']}</td>
                <td>{month['botRelative']}</td>
              </tr>
            </tbody>
          </table>
      </div>
    );
  };

  renderTopUsers = (topUsers) => {
    var hour = topUsers['hour'];
    if (hour === 0) {
      hour = [];
    }
    else {
      hour = hour['topUser'];
    }

    var day = topUsers['day'];
    if (day === 0) {
      day = [];
    }
    else {
      day = day['topUser'];
    }

    var week = topUsers['week'];
    if (week === 0) {
      week = [];
    }
    else {
      week = week['topUser'];
    }

    var month = topUsers['month'];
    if (month === 0) {
      month = [];
    }
    else {
      month = month['topUser'];
    }

    var hourData = hour.map((userData, index) => {
      return <td key={index}>{userData['user']}, {userData['count']}</td>
    });

    var dayData = day.map((userData, index) => {
      return <td key={index}>{userData['user']}, {userData['count']}</td>
    });

    var weekData = week.map((userData, index) => {
      return <td key={index}>{userData['user']}, {userData['count']}</td>
    });

    var monthData = month.map((userData, index) => {
      return <td key={index}>{userData['user']}, {userData['count']}</td>
    });



    return (
      <div>
        <table className="table">
            <thead>
              <tr>
                <th scope="col">Time Frame</th>
                <th scope="col">User1</th>
                <th scope="col">User2</th>
                <th scope="col">User3</th>
                <th scope="col">User4</th>
                <th scope="col">User5</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>hour</td>
                {hourData}
              </tr>
              <tr>
                <td>day</td>
                {dayData}
              </tr>
              <tr>
                <td>week</td>
                {weekData}
              </tr>
              <tr>
                <td>month</td>
                {monthData}
              </tr>
            </tbody>
          </table>
      </div>
    );
  };


  renderTopPages = (topPages) => {
    var hour = topPages['hour'];
    if (hour === 0) {
      hour = [];
    }
    else {
      hour = hour['topPage'];
    }
    var day = topPages['day'];
    if (day === 0) {
      day = [];
    }
    else {
      day = day['topPage'];
    }
    var week = topPages['week'];
    if (week === 0) {
      week = [];
    }
    else {
      week = week['topPage'];
    }
    var month = topPages['month'];
    if (month === 0) {
      month = [];
    }
    else {
      month = month['topPage'];
    }

    var hourData = hour.map((page, index) => {
      return <td key={index}>{page['page']}, {page['count']}</td>
    });

    var dayData = day.map((page, index) => {
      return <td key={index}>{page['page']}, {page['count']}</td>
    });

    var weekData = week.map((page, index) => {
      return <td key={index}>{page['page']}, {page['count']}</td>
    });

    var monthData = month.map((page, index) => {
      return <td key={index}>{page['page']}, {page['count']}</td>
    });

    return (
      <div>
        <table className="table">
            <thead>
              <tr>
                <th scope="col">Time Frame</th>
                <th scope="col">page1</th>
                <th scope="col">page2</th>
                <th scope="col">page3</th>
                <th scope="col">page4</th>
                <th scope="col">page5</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>hour</td>
                {hourData}
              </tr>
              <tr>
                <td>day</td>
                {dayData}
              </tr>
              <tr>
                <td>week</td>
                {weekData}
              </tr>
              <tr>
                <td>month</td>
                {monthData}
              </tr>
            </tbody>
          </table>
      </div>
    );
  };

  displayBlock = (key) => {
    const data = this.state.data;
      const topUsers = data[key]['topUsers'];
      const topBots = data[key]['topBots'];
      const topPages = data[key]['topPages'];

      const newData = data[key]['new'];
      const editData = data[key]['edit'];
      const revertData = data[key]['revert'];

      return (
        <div>
          <h1>{key}</h1>
          <div className="container1">
            <div className="row1">
              <div className="col1">
                <h2>New Page</h2>
                  {this.renderStats(newData)}
              </div>
              <div className="col1">
                <h2>Edit Page</h2>
                  {this.renderStats(editData)}
              </div>
              <div className="col1">
                <h2>Revert Page</h2>
                  {this.renderStats(revertData)}
              </div>
            </div>

            <div className="row1">
              <div className="col1">
                <h2>Top Users</h2>
                  {this.renderTopUsers(topUsers)}
                </div>
                <div className="col1">
                  <h2>Top Bots</h2>
                    {this.renderTopUsers(topBots)}
                </div>
                <div className="col1">
                  <h2>Top Pages</h2>
                    {this.renderTopPages(topPages)}
                </div>
            </div>
          </div>
        </div>
      )

  };

  renderData = () => {
    const data = this.state.data;
    return Object.keys(data).map(key => {
      return (
        <div className={key}>
          {this.displayBlock(key)}
        </div>
        );
    });
  };

  

  render() {
    return (
      <div className="App">
       <div className="App-body">
         <div className="App-body-data">
           {this.renderData()}
         </div>
       </div>
     </div>
    );
  }

}


export default App;
