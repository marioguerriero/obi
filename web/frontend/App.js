import React, { Component } from 'react';
import './App.css';

import Header from './Header'
import LoginForm from './LoginForm'
import ClustersList from './ClustersList'

import config from './config'
import utils from './utils'

class App extends Component {
  constructor(props) {
    super(props);
      this.state = {
        token: localStorage.getItem(config.OBI_TOKEN_KEY)
      };

      this.loginSuccess = this.loginSuccess.bind(this);
      this.loginFail = this.loginFail.bind(this);
      this.watchLocalStorage = this.watchLocalStorage.bind(this);
  }

  componentWillMount() {
    window.addEventListener("storage", this.watchLocalStorage, false);
  }

  componentWillUnmount() {
    window.removeEventListener("storage", this.watchLocalStorage);
  }

  watchLocalStorage() {
    if(localStorage.getItem(config.OBI_TOKEN_KEY) !== this.state.token) {
        this.setState({
            token: localStorage.getItem(config.OBI_TOKEN_KEY)
        })
    }
  }

  loginSuccess() {
    // Update user state to trigger re-rendering
    this.setState({
        token: localStorage.getItem(config.OBI_TOKEN_KEY)
    })
  }

  loginFail(err) {
    utils.clearToken()
    // TODO: show error message
  }

  render() {

    let body = <div/>;

    if(!this.state.token) {
      // Render login form if user is not authorized
      body = <LoginForm
          onLoginSuccess={this.loginSuccess}
          onLoginFail={this.loginFail}/>
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
