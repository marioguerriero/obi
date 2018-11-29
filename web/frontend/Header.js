import React, { Component } from 'react';
import { Navbar, Nav, NavItem } from 'react-bootstrap';
import './App.css';

export default class extends Component {
    render() {
        return (
            <Navbar>
                <Navbar.Header>
                    <Navbar.Brand>
                        <a href="#home">OBI</a>
                    </Navbar.Brand>
                </Navbar.Header>
                <Nav pullRight>
                    {/*<NavItem eventKey={1} href="#">*/}
                        {/*Login*/}
                    {/*</NavItem>*/}
                </Nav>
            </Navbar>
        );
    }
}
