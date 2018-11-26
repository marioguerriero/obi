import React, { Component } from 'react';
import './App.css';

import LoginForm from './LoginForm'
import ClustersList from './ClustersList'

class App extends Component {
  constructor(props) {
    super(props);
      this.state = {
        token: localStorage.getItem('obi-auth-token')
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
      body = <ClustersList />
    }

    return (
      <div className="App">
        <header className="App-header">
          Header
        </header>

        <div className="App-body">
          {body}
        </div>

        <footer className="App-footer">
          Footer
        </footer>
      </div>
    );
  }
}

export default App;
