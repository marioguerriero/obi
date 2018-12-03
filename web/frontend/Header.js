import React, { Component } from 'react';
import { Navbar, Nav, NavItem, Row } from 'react-bootstrap';
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
                    <Navbar.Brand className="OBI-brand">
                        {/*<Row>*/}
                            {/*<a href="#home">*/}
                                {/*<img src="/logo.jpg" alt={"OBI Logo"} className="OBI-logo" />*/}
                                OBI
                            {/*</a>*/}
                        {/*</Row>*/}
                    </Navbar.Brand>
                </Navbar.Header>
                <Nav pullRight>
                    {logout}
                </Nav>
            </Navbar>
        );
    }
}
