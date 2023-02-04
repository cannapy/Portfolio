import React from 'react';
import {BrowserRouter,Switch, Router, Link} from 'react-router-dom';
import {useState} from 'react'
import {Home} from './pages/Home'
import {About} from './pages/About'
import {Articles} from './pages/Articles'


const App = () =>{

    return(
        <BrowserRouter>
            <h1>Server-Side Rendering Example</h1>
            <ul>
                <li>
                    <Link to="/">Home</Link>
                </li>
                <li>
                    <Link to="/About">About</Link>
                </li>
                <li>
                    <Link to ="/Articles">Articles</Link>
                </li>
            </ul>
            <Switch>
                <Route path = "/" exact>
                    <Home />
                </Route>
                <Route path = "/Articles">
                    <About/>
                </Route>
                <Route path = "/About">
                    <About />
                </Route>
            
            </Switch>
        </BrowserRouter>
        
    )
}

export default App