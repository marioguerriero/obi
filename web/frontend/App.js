import React, { Component } from 'react';
import './App.css';

import Header from './Header'
import LoginForm from './LoginForm'
import ClustersList from './ClustersList'

import config from './config'

class App extends Component {
  constructor(props) {
    super(props);
      this.state = {
        token: localStorage.getItem(config.OBI_TOKEN_KEY)
      }
  }

  async componentDidMount() {
    const response = await fetch('/ping', {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      }
    });
    const body = await response.text();
    console.log(body)
  }

  render() {

    let body = <div/>;

    if(!this.state.token) {
      // Render login form if user is not authorized
      body = <LoginForm/>
    }
    else {
      // Otherwise display clusters list
      body = <ClustersList token={this.state.token} />
    }

    return (
      <div className="App">
        <Header/>

        <div className="App-body">
          {body}
        </div>

        <footer className="App-footer">

        </footer>
      </div>
    );
  }
}

export default App;
