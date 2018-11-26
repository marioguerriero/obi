import React, { Component } from 'react';
import './App.css';

import JobItem from './JobItem'

export default class extends Component {
    render() {
        return (
            <div className="ClusterItem">
                <JobItem/>
            </div>
        );
    }
}
