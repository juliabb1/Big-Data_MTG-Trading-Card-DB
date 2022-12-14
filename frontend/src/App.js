import React, { useState, useEffect } from 'react';
import axios from 'axios';

import './App.css';

/**App
 * contains the search system logic and the elements that should be displayed in the frontend.
 * The search system can be used to search for a card via its name or via it's multiverseid
 * @returns homepage of frontend
 */
function App() {
  // entries in mtg_cards table
  const [mtg_cards, setMtg_cards] = useState([]);
  
    // function to fetch the data from 'fantasy_football' database
    const getData = async () => {
      try {
        const res = await axios.get('http://localhost:3001/mtg_cards')
        await setMtg_cards(res.data.data);
      } catch (error) {
        console.log(error);
      }
    };
  // React Hook that executes the fetch function on the first render 
  useEffect(() => {
    getData();
  }, []);

  // the value of the search field 
  const [name, setName] = useState('');
  // the search result
  const [foundCards, setFoundCards] = useState(mtg_cards);
  const filter = (e) => {
    const search_val = e.target.value;

    // Check fo Name input
    if (search_val !== '' && isNaN(search_val)) {
      const results = mtg_cards.filter((mtg_card) => {
        return mtg_card.name.toLowerCase().startsWith(search_val.toLowerCase());
      });
      setFoundCards(results);
    } 
    // Check fo number input
    else if (search_val !== '' && !isNaN(search_val)) {
      var search_id = parseInt(search_val)
      const results = mtg_cards.filter((mtg_card) => {
        return mtg_card.multiverseid.startsWith(search_id.toString());
      });
      setFoundCards(results);
    } 
    // If the text field is empty, show no
    else {
      setFoundCards([]);
    }
    setName(search_val);
  };
  var setBlankScreen = true;

  // what should be displayed
  return (
    <div className="container">
      <input type="search" 
      value={name}
        onChange={filter}
        className="input"
        placeholder="Search for..."
      />

      <div className="cards-list">
        {foundCards && foundCards.length > 0 ? (
          foundCards.map((mtg_card) => (
            <li key={mtg_card.multiverseid} className="cards">
              <span className="cards-imageurl">
                <img className="cards-img" alt="MTG card" src={mtg_card.imageUrl}/>
                </span>
              <span className="cards-name">{mtg_card.name}</span>
              <span className="cards-multiverseid">{mtg_card.multiverseid}</span>
            </li>
          ))
        ) : ( () => {
          if (setBlankScreen === true) {
            setBlankScreen = false;
            return <h1 className='no-results'>Search for a MTG Card!</h1>
          }
          else
            return <h1 className='no-results'>No results found!</h1>
        })()
      }
      </div>
    </div>
  );
}

export default App;