import React, { Component } from 'react';
import './App.css';

export default class extends Component {
    render() {
        const job = this.props.job;

        return (
            <div className="JobItem">
                {job.status}
            </div>
        );
    }
}
