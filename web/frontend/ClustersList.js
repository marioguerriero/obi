import React, { Component } from 'react';
import './App.css';

import ClusterItem from './ClusterItem'

export default class extends Component {
    render() {
        return (
            <div className="ClustersList">
                <ClusterItem/>
            </div>
        );
    }
}
