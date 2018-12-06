// Copyright 2018 Delivery Hero Germany
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.

import React, { Component } from 'react';
import { Navbar, Nav, NavItem } from 'react-bootstrap';
import './App.css';

import config from './config'

export default class Header extends Component {
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
