import React, { Component } from 'react';
import { Navbar, Nav, NavItem } from 'react-bootstrap';
import './App.css';

import config from './config'

export default class extends Component {
    render() {
        // Check if the user is already logged in
        const isLoggedIn = localStorage.getItem(config.OBI_TOKEN_KEY) != null;
        let logout = '';
        if(isLoggedIn) {
            logout = <NavItem eventKey={1} href="#" onClick={this.props.onLogout}>
                Logout
            </NavItem>
        }

        return (
            <Navbar>
                <Navbar.Header>
                    <Navbar.Brand>
                        <a href="#home">OBI</a>
                    </Navbar.Brand>
                </Navbar.Header>
                <Nav pullRight>
                    {logout}
                </Nav>
            </Navbar>
        );
    }
}
