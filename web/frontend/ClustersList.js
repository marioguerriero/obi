import React, { Component } from 'react';
import './App.css';

import ClusterItem from './ClusterItem'

export default class extends Component {
    async fetchClusters() {
        const response = await fetch('/clusters', {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + this.props.token
            }
        });
        this.clusters = await response.json();
    }

    componentWillMount() {
        this.fetchClusters()
    }

    componentWillUnmount() {
        this.fetchClusters()
    }

    render() {
        // Create clusters list
        let clusters = this.clusters.map((cluster) =>
            <ClusterItem cluster={cluster} />
        );

        return (
            <div className="ClustersList">
                {clusters}
            </div>
        );
    }
}
