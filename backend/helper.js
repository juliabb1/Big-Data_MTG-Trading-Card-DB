/**getOffset
 * gets the offset of the page.
 * @param {*} currentPage 
 * @param {*} listPerPage 
 * @returns page offset
 */
function getOffset(currentPage = 1, listPerPage) {
    return (currentPage - 1) * [listPerPage];
  }
  
  /**emptyOrRows
   * Checks if there is data in a row or not.
   *  @returns rows or empty list
   */
  function emptyOrRows(rows) {
    if (!rows) {
      return [];
    }
    return rows;
  }
  
  module.exports = {
    getOffset,
    emptyOrRows
  }